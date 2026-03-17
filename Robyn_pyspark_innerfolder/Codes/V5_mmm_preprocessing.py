import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from preprocessing.V2_preprocessing_executor import (
    run_agg_preprocessing,
    run_outlier_post_aggregation,
    run_missing_value_post_outlier,
    run_date_continuity,
)
from preprocessing.executor_functions.metadata_loader import load_metric_metadata
from preprocessing.executor_functions.V2_funnel_metrics_executor import execute_funnel_metrics
from signals.candidate_narrowing import execute_candidate_narrowing  # NOTE: not converted in this session — verify it accepts a Spark DataFrame
from config import (
    RAW_DATA_PATH,
    METADATA_PATH,
    GROUPBY_COLS,
    DATE_COL,
    OUTPUT_DATA_PATH,
    OUTPUT_META_PATH,
    OUTPUT_REGISTRY_PATH,
    START_FREQ,
    END_FREQ,
)


# --------------------------------------------------
# SparkSession — created once at pipeline entry point
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("MMM_Preprocessing_Pipeline") \
    .getOrCreate()


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def _normalize_columns(df: DataFrame) -> DataFrame:
    """
    Strip whitespace and lowercase all column names.
    Replicates: df.columns = df.columns.str.strip().str.lower()
    PySpark column names are immutable — each one must be renamed individually.
    """
    for col in df.columns:
        normalized = col.strip().lower()
        if normalized != col:
            df = df.withColumnRenamed(col, normalized)
    return df


def _shape(df: DataFrame) -> tuple:
    """
    Replicates df.shape for diagnostic print statements.
    df.count() triggers a Spark action — used only for logging.
    """
    return (df.count(), len(df.columns))


# --------------------------------------------------
# Load raw data
# --------------------------------------------------



df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_DATA_PATH)
)

# Normalize all column names: strip + lowercase
# Replicates: df_raw.columns = df_raw.columns.str.strip().str.lower()
df_raw = _normalize_columns(df_raw)

# Normalize config strings to match lowercased column names — pure Python, unchanged
GROUPBY_COLS = [c.lower() for c in GROUPBY_COLS]
DATE_COL     = DATE_COL.lower()




# --------------------------------------------------
# Load metric metadata
# --------------------------------------------------



# load_metric_metadata returns a Pandas DataFrame — intentional and permanent.
# Metadata is a small driver-side config table used for dispatch logic in
# every executor. It must stay as Pandas after loading.
# field_name normalization is already performed inside load_metric_metadata.
df_metadata = load_metric_metadata(source=METADATA_PATH, spark=spark)

# Filter metadata to only columns that exist in raw data.
# df_raw.columns is a plain Python list in PySpark — .isin() works identically.
df_metadata = df_metadata[df_metadata["field_name"].isin(df_raw.columns)]




# --------------------------------------------------
# Step 1 — Outlier Treatment (on raw data, pre-continuity)
# --------------------------------------------------



df_outlier_treated = run_outlier_post_aggregation(
    df=df_raw,
    metadata_df=df_metadata,
    groupby_cols=GROUPBY_COLS,
    date_col=DATE_COL          # added in PySpark conversion for window ordering
)



# --------------------------------------------------
# Step 2 — Enforce Date Continuity
# --------------------------------------------------



df_continuity_enforced = run_date_continuity(
    raw_df=df_outlier_treated,
    groupby_cols=GROUPBY_COLS,
    date_col=DATE_COL,
    frequent=START_FREQ
)




# --------------------------------------------------
# Step 3 — Missing Value Imputation
# --------------------------------------------------



df_missing_imputed = run_missing_value_post_outlier(
    df=df_continuity_enforced,
    metadata_df=df_metadata,
    groupby_cols=GROUPBY_COLS,
    date_col=DATE_COL          # added in PySpark conversion for window ordering
)


# --------------------------------------------------
# Step 4 — Aggregation
# --------------------------------------------------


df_agg = run_agg_preprocessing(
    raw_df=df_missing_imputed,
    metadata_df=df_metadata,
    groupby_cols=[DATE_COL] + GROUPBY_COLS,
    date_col=DATE_COL,
    start_freq=START_FREQ,
    end_freq=END_FREQ
)



# --------------------------------------------------
# Step 5 — Funnel Metrics
# --------------------------------------------------



# execute_funnel_metrics returns:
#   df_agg       → Spark DataFrame  (main data with derived ratio columns added)
#   new_metadata → Pandas DataFrame (metadata with new derived metric rows appended)
df_agg, new_metadata = execute_funnel_metrics(df_agg, df_metadata)

# --------------------------------------------------
# Step 6 — Candidate Narrowing
# --------------------------------------------------


# NOTE: execute_candidate_narrowing was not part of this conversion session.
# If its internals expect a Pandas DataFrame, replace df_agg with df_agg.toPandas().
# If it has been independently converted to PySpark, pass df_agg directly.
registry = execute_candidate_narrowing(df=df_agg, metadata_df=new_metadata)



# --------------------------------------------------
# Save outputs
# --------------------------------------------------

# Main aggregated data — written via Spark.
# coalesce(1) produces a single CSV file, matching the original to_csv() behaviour.
# For large datasets, consider .write.parquet() or Delta format instead.
df_agg \
    .coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(OUTPUT_DATA_PATH)

# Metadata — still a Pandas DataFrame, saved directly with Pandas
new_metadata.to_csv(OUTPUT_META_PATH, index=False)

# Registry — pure Python dict.
# Replaces: pd.Series(registry).to_json(OUTPUT_REGISTRY_PATH, indent=2)
# pd.Series was being used purely as a JSON serializer — stdlib json.dump is correct.
with open(OUTPUT_REGISTRY_PATH, "w") as f:
    json.dump(registry, f, indent=2)



# --------------------------------------------------
# Final summary
# --------------------------------------------------

# _shape() called once and reused — avoids triggering two count() actions
agg_shape = _shape(df_agg)

