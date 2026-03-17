from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

from Robyn_Premodelling.best_exposure import get_best_exposure_registry_spark
from Robyn_Premodelling.conforming_for_modelling import prepare_modelling_data_spark

from config import (
    OUTPUT_DATA_PATH,
    OUTPUT_META_PATH,
    OUTPUT_REGISTRY_PATH,
    OUTPUT_PREMODELLING_REGISTRY_PATH,
    OUTPUT_PREMODELLING_DATA_PATH,
    GROUPBY_COLS,
    DATE_COL
)

# ──────────────────────────────────────────────────────────────────────────────
# 0. SPARK SESSION
# ──────────────────────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("Robyn_Premodelling_Pipeline") \
    .getOrCreate()

# Optional — suppress verbose Spark logs
spark.sparkContext.setLogLevel("WARN")

# ──────────────────────────────────────────────────────────────────────────────
# 1. LOAD DATA & REGISTRY
# ──────────────────────────────────────────────────────────────────────────────



# ── Load main data as Spark DataFrame ────────────────────────────────────────
# inferSchema=True handles numeric columns automatically
# header=True uses first row as column names
df_input_spark = spark.read.csv(
    OUTPUT_DATA_PATH,
    header=True,
    inferSchema=True
)

# ── Load metadata as pandas — it's small and only used for logging ────────────
df_metadata = spark.read.csv(
    OUTPUT_META_PATH,
    header=True,
    inferSchema=True
)

# ── Load registry — JSON, stays in Python as a dict ──────────────────────────
with open(OUTPUT_REGISTRY_PATH, 'r') as f:
    registry_input = json.load(f)

# ── Normalise column names to lowercase ──────────────────────────────────────
# Spark equivalent of df.columns.str.strip().str.lower()
GROUPBY_COLS = [c.lower() for c in GROUPBY_COLS]
DATE_COL     = DATE_COL.lower()

df_input_spark = df_input_spark.toDF(
    *[c.strip().lower() for c in df_input_spark.columns]
)

# ── Summary print ─────────────────────────────────────────────────────────────
# count() is an action — called once here, not repeated downstream
row_count = df_input_spark.count()
col_count = len(df_input_spark.columns)



# ──────────────────────────────────────────────────────────────────────────────
# 2. BEST EXPOSURE SELECTION
#    Correlates each channel's exposure candidates against sales_revenue
#    using Spark's distributed stat.corr() — no collect() on full data
# ──────────────────────────────────────────────────────────────────────────────



modelling_registry, correlation_report = get_best_exposure_registry_spark(
    df_input_spark = df_input_spark,
    registry_input = registry_input,
)




# ──────────────────────────────────────────────────────────────────────────────
# 3. CONFORM DATA FOR ROBYN
#    Builds model-ready Spark DataFrame with segment, dep_var,
#    controls, exposures, spends
#    No scaling — Robyn handles internally
# ──────────────────────────────────────────────────────────────────────────────



df_model_ready_spark, updated_registry = prepare_modelling_data_spark(
    df_input_spark     = df_input_spark,
    modelling_registry = modelling_registry,
    date_col           = DATE_COL,
    groupby_cols       = GROUPBY_COLS,
)



# ──────────────────────────────────────────────────────────────────────────────
# 4. SAVE OUTPUTS
#    Spark writes CSVs as partitioned directories by default
#    coalesce(1) forces single file output — matches pandas to_csv behaviour
#    Use Delta format on Databricks for better performance if available
# ──────────────────────────────────────────────────────────────────────────────



# ── Save model-ready data ────────────────────────────────────────────────────
# coalesce(1) → single CSV file, header=True → includes column names
# mode='overwrite' → replaces existing output
df_model_ready_spark \
    .coalesce(1) \
    .write \
    .mode('overwrite') \
    .option('header', True) \
    .csv(OUTPUT_PREMODELLING_DATA_PATH)

# ── Save updated registry — stays as JSON, written from Python ────────────────
with open(OUTPUT_PREMODELLING_REGISTRY_PATH, 'w') as f:
    json.dump(updated_registry, f, indent=2)



# ──────────────────────────────────────────────────────────────────────────────
# 5. SUMMARY
# ──────────────────────────────────────────────────────────────────────────────

# ── Final shape — single count() action ──────────────────────────────────────
final_rows = df_model_ready_spark.count()
final_cols = len(df_model_ready_spark.columns)


# ──────────────────────────────────────────────────────────────────────────────
# 6. HANDOFF TO ROBYN
#    Robyn requires pandas — convert here, after all Spark processing is done
#    toPandas() is the single collect() point — do this as late as possible
# ──────────────────────────────────────────────────────────────────────────────

df_model_ready_pandas = df_model_ready_spark.toPandas()
