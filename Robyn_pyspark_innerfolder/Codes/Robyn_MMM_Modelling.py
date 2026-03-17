from robyn_modelling_functions import (
    load_holidays,
    parse_registry,
    run_segment_pipeline,
)
import os
import json
import multiprocessing
import pandas as pd
import traceback

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    OUTPUT_PREMODELLING_DATA_PATH,
    OUTPUT_PREMODELLING_REGISTRY_PATH,
    DATE_COL,
    PROPHET_COUNTRY,
    ROBYN_OUTPUT_PATH,
    ITERATIONS,
    TRIALS,
    PARETO_FRONTS,
    MIN_CANDIDATES,
    ADSTOCK,
)

# ════════════════════════════════════════════════════════════════════════════
# SPARK SESSION
# On Databricks this already exists — SparkSession.builder is a no-op
# ════════════════════════════════════════════════════════════════════════════
spark = SparkSession.builder \
    .appName("Robyn_Modelling_Pipeline") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ════════════════════════════════════════════════════════════════════════════
# HARDCODED FOR NOW
# TODO: move to registry as 'global_control_signs'
# ════════════════════════════════════════════════════════════════════════════
CONTEXT_SIGNS = ['negative', 'positive', 'negative']

# ════════════════════════════════════════════════════════════════════════════
# DYNAMIC CORES
# On Databricks — use spark default parallelism as a hint
# ════════════════════════════════════════════════════════════════════════════
CORES = multiprocessing.cpu_count()

# ════════════════════════════════════════════════════════════════════════════
# 1. LOAD — Spark reads from DBFS/cloud storage
#    toPandas() done immediately after — Robyn needs pandas
#    Data is small enough (weekly MMM data) that this is fine
# ════════════════════════════════════════════════════════════════════════════


df_input_spark = spark.read.csv(
    OUTPUT_PREMODELLING_DATA_PATH,
    header=True,
    inferSchema=True
)

# ── Handoff to pandas — Robyn requires pandas input ──────────────────────
# This is the single collect() point for input data
df_input = df_input_spark.toPandas()
df_input['date'] = pd.to_datetime(df_input[DATE_COL.lower()])

# ── Registry stays in Python — small JSON dict ────────────────────────────
with open(OUTPUT_PREMODELLING_REGISTRY_PATH, 'r') as f:
    modelling_registry = json.load(f)


# ════════════════════════════════════════════════════════════════════════════
# 2. PARSE REGISTRY — pure Python, no Spark needed
# ════════════════════════════════════════════════════════════════════════════


dep_var, paid_media_spends, paid_media_vars, context_vars = parse_registry(modelling_registry)

if len(CONTEXT_SIGNS) != len(context_vars):
    raise ValueError(
        f"[ERROR] CONTEXT_SIGNS length ({len(CONTEXT_SIGNS)}) "
        f"does not match context_vars length ({len(context_vars)}). "
        f"context_vars: {context_vars}"
    )

dt_holidays = load_holidays()

# ════════════════════════════════════════════════════════════════════════════
# 3. RUN PIPELINE PER SEGMENT
#    Robyn is R-backed — must run on driver node in pandas
#    Cannot be distributed to Spark workers without R on each worker
#    On Databricks: R is available on driver by default
#    For multi-segment parallelism: use Databricks Jobs with segment-level
#    job parameters instead of Spark worker distribution
# ════════════════════════════════════════════════════════════════════════════
df_input = df_input[df_input['segment'] == 'Region2_Category1_Category1Brand1']
segments = df_input['segment'].unique().tolist()


pd.set_option('display.float_format', '{:.4f}'.format)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 80)

best_per_segment = {}
all_params       = {}
all_scores       = []

for seg in segments:


    df_seg     = df_input[df_input['segment'] == seg].drop(columns=['segment']).reset_index(drop=True)
    output_dir = os.path.join(ROBYN_OUTPUT_PATH, seg)

    best_sol_id, scored_df, params, _ = run_segment_pipeline(
        seg=seg,
        df_seg=df_seg,
        dep_var=dep_var,
        paid_media_spends=paid_media_spends,
        paid_media_vars=paid_media_vars,
        context_vars=context_vars,
        context_signs=CONTEXT_SIGNS,
        output_dir=output_dir,
        dt_holidays=dt_holidays,
        prophet_country=PROPHET_COUNTRY,
        iterations=ITERATIONS,
        trials=TRIALS,
        cores=CORES,
        adstock=ADSTOCK,
        pareto_fronts=PARETO_FRONTS,
        min_candidates=MIN_CANDIDATES,
    )

    if best_sol_id is None:
        
        continue

    best_per_segment[seg] = best_sol_id
    if params:
        all_params[seg] = params
    if scored_df is not None:
        
        all_scores.append(scored_df)

# ════════════════════════════════════════════════════════════════════════════
# 4. SAVE OUTPUTS — Spark writes to DBFS/cloud storage
# ════════════════════════════════════════════════════════════════════════════



os.makedirs(ROBYN_OUTPUT_PATH, exist_ok=True)

# ── Scores CSV — use Spark writer ─────────────────────────────────────────
if all_scores:
    scores_df_pandas = pd.concat(all_scores, ignore_index=True)
    scores_df_spark  = spark.createDataFrame(scores_df_pandas)
    scores_path      = os.path.join(ROBYN_OUTPUT_PATH, 'model_selection_scores')
    scores_df_spark \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .option('header', True) \
        .csv(scores_path)
    

# ── Best selections JSON — small dict, stays Python ──────────────────────
selections_path = os.path.join(ROBYN_OUTPUT_PATH, 'best_model_selections.json')
with open(selections_path, 'w') as f:
    json.dump(best_per_segment, f, indent=2)


# ── Hill params + per-segment CSVs ───────────────────────────────────────
hill_params_all = {}
for seg, p in all_params.items():
    if p is None:
        continue
    seg_dir = os.path.join(ROBYN_OUTPUT_PATH, seg)
    os.makedirs(seg_dir, exist_ok=True)

    hill_params_all[seg] = {
        'sol_id':         p['sol_id'],
        'fit_metrics':    p['fit_metrics'],
        'hill_params':    p['hill_params'],
        'mm_params':      p['mm_params'],
        'channel_decomp': p['channel_decomp'],
    }

    # ── Weekly decomp — Spark writer ──────────────────────────────────────
    weekly_decomp_spark = spark.createDataFrame(p['weekly_decomp'])
    weekly_decomp_spark \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .option('header', True) \
        .csv(os.path.join(seg_dir, 'weekly_decomp'))

    # ── Raw data — Spark writer ────────────────────────────────────────────
    raw_data_spark = spark.createDataFrame(p['raw_data'])
    raw_data_spark \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .option('header', True) \
        .csv(os.path.join(seg_dir, 'raw_data'))

# ── Hill params JSON — small dict, stays Python ───────────────────────────
hill_params_path = os.path.join(ROBYN_OUTPUT_PATH, 'hill_params_all_segments.json')
with open(hill_params_path, 'w') as f:
    json.dump(hill_params_all, f, indent=2)


