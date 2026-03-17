from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import copy
import json


def get_best_exposure_registry_spark(df_input_spark, registry_input):
    """
    PySpark version of get_best_exposure_registry.

    Evaluates all exposure metrics for each channel against the target variable
    using Pearson correlation, and returns a modelling registry where each channel
    has exactly one exposure — the best correlated one.

    Args:
        df_input_spark  : PySpark DataFrame with all exposure columns
        registry_input  : candidate registry (output of preprocessing pipeline)

    Returns:
        modelling_registry : updated registry with single best exposure per channel
        correlation_report : dict of {channel: {metric: correlation}} for reference

    Notes:
        - PySpark's corr() is computed distributed — no .toPandas() needed
        - corr() returns None for zero-variance columns — handled explicitly
        - Registry manipulation stays in Python (it's a small dict, no need for Spark)
    """
    modelling_registry = copy.deepcopy(registry_input)

    global_response = registry_input.get('global_response', [])
    if not global_response:
        raise ValueError("[ERROR] 'global_response' key is missing or empty in registry.")
    target_col = global_response[0]

    # Validate target column exists in Spark DataFrame
    if target_col not in df_input_spark.columns:
        raise ValueError(f"[ERROR] Target column '{target_col}' not found in DataFrame.")

    correlation_report = {}

    print(f"[BEST EXPOSURE] Target variable : '{target_col}'")
    print(f"[BEST EXPOSURE] Channels        : {list(registry_input.get('channels', {}).keys())}\n")

    for channel, metrics in registry_input.get('channels', {}).items():
        exposure_metrics = metrics.get('exposure', [])

        if not exposure_metrics:
            print(f"[{channel.upper()}] No exposure metrics defined — skipping.")
            continue

        best_metric   = None
        highest_corr  = -float('inf')
        channel_corrs = {}

        for metric in exposure_metrics:

            # Validate column exists in Spark DataFrame
            if metric not in df_input_spark.columns:
                print(f"[{channel.upper()}] Column '{metric}' not found in data — skipping.")
                continue

            # ── PySpark distributed correlation ───────────────────────────
            # stat.corr() runs on the cluster — no collect() or toPandas()
            corr = df_input_spark.stat.corr(metric, target_col)

            # Handle None (zero-variance or null columns)
            if corr is None or corr != corr:  # NaN check without importing math
                corr = 0.0

            channel_corrs[metric] = round(corr, 4)

            if corr > highest_corr:
                highest_corr = corr
                best_metric  = metric

        correlation_report[channel] = channel_corrs

        if best_metric:
            modelling_registry['channels'][channel]['exposure'] = [best_metric]
            print(f"  [{channel.upper():20s}] Best: '{best_metric}' (corr: {round(highest_corr, 4)})")
        else:
            print(f"  [{channel.upper():20s}] No valid exposure metric found.")

    print("\n[BEST EXPOSURE] Modelling registry generated successfully.")
    return modelling_registry, correlation_report