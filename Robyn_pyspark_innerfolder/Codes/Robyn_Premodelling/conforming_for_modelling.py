from pyspark.sql import functions as F
from pyspark.sql.window import Window
import copy


def prepare_modelling_data_spark(df_input_spark, modelling_registry, date_col, groupby_cols):
    """
    PySpark version of prepare_modelling_data.

    Conforms preprocessed data for Robyn modelling.

    - Creates a 'segment' column from groupby_cols
    - Filters to only columns needed for Robyn: date, segment, dep_var, controls, exposures, spends
    - No scaling — Robyn handles internally
    - Sorted chronologically per segment

    Args:
        df_input_spark     : PySpark DataFrame (preprocessed)
        modelling_registry : registry with best exposure already selected
        date_col           : name of date column (str)
        groupby_cols       : segment columns e.g. ['region', 'category', 'brand']

    Returns:
        df_model_ready : PySpark DataFrame — model-ready for Robyn
        registry       : updated registry (unchanged, passed through)

    Notes:
        - concat_ws used for segment creation — handles nulls gracefully
        - sort within segment uses orderBy — Spark sorts globally but deterministically
        - Column dedup preserves order using a Python seen set
        - Registry manipulation stays in Python — small dict, no benefit to distributing
    """
    registry = copy.deepcopy(modelling_registry)

    # ── 1. Validate groupby columns ───────────────────────────────────────────
    spark_cols       = df_input_spark.columns
    missing_seg_cols = [c for c in groupby_cols if c not in spark_cols]
    if missing_seg_cols:
        raise ValueError(f"[ERROR] Groupby columns not found in data: {missing_seg_cols}")

    # ── 2. Segment column ─────────────────────────────────────────────────────
    # concat_ws handles nulls gracefully — null values become empty string
    # cast to string first to handle numeric segment cols
    string_cols = [F.col(c).cast('string') for c in groupby_cols]
    df = df_input_spark.withColumn('segment', F.concat_ws('_', *string_cols))

    # Print segment summary — requires collect() but only on distinct values (tiny)
    segments = sorted([row['segment'] for row in df.select('segment').distinct().collect()])
    print(f"[CONFORM] Segments found : {len(segments)} → {segments}")

    # ── 3. Sort chronologically per segment ───────────────────────────────────
    df = df.orderBy(['segment', date_col])

    # ── 4. Extract columns from registry ──────────────────────────────────────
    target   = registry.get('global_response', [])
    controls = registry.get('global_controls', [])

    exposures, spends = [], []
    for channel_data in registry.get('channels', {}).values():
        exposures.extend(channel_data.get('exposure', []))
        spends.extend(channel_data.get('spend', []))

    # ── 5. Warn on missing columns ────────────────────────────────────────────
    all_expected = set(target + controls + exposures + spends)
    missing      = all_expected - set(spark_cols)
    if missing:
        print(f"[CONFORM] WARNING — in registry but not in data: {missing}")

    # ── 6. Build model-ready column list ──────────────────────────────────────
    model_cols = [date_col, 'segment'] + target + controls + exposures + spends

    # Deduplicate while preserving order, skip missing columns
    seen, model_cols_clean = set(), []
    for c in model_cols:
        if c not in seen and c in spark_cols + ['segment']:
            seen.add(c)
            model_cols_clean.append(c)

    # ── 7. Select final columns ───────────────────────────────────────────────
    df_model_ready = df.select(model_cols_clean)

    # ── Summary ───────────────────────────────────────────────────────────────
    # count() triggers an action — only called once for the summary print
    row_count = df_model_ready.count()
    col_count = len(df_model_ready.columns)



    return df_model_ready, registry