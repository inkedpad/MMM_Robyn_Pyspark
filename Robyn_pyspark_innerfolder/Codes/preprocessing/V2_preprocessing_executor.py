from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from preprocessing.executor_functions.aggregation_executor import aggregate_metric
from preprocessing.executor_functions.date_time_executor import execute_date_time_normalization
from preprocessing.functions.V2_date_time import enforce_date_continuity
from preprocessing.executor_functions.validation_executor import execute_negativity_validation
from preprocessing.executor_functions.V2_Outlier_Executor import execute_outlier_treatment
from preprocessing.executor_functions.flag_executor import execute_flag_normalization
from preprocessing.executor_functions.V2_missing_value_executor import execute_missing_value_treatment


# ------------------------------------------------------------------
# For Aggregation and Consistency corrections
# ------------------------------------------------------------------

def run_date_continuity(
    raw_df: DataFrame,
    groupby_cols: list,
    date_col: str,
    frequent: str
) -> DataFrame:

    df_continued = enforce_date_continuity(
        df=raw_df,
        date_col=date_col,
        groupby_cols=groupby_cols,
        freq=frequent
    )
    return df_continued


def run_agg_preprocessing(
    raw_df: DataFrame,
    metadata_df,               # Pandas DataFrame — driver-side metadata
    groupby_cols: list,
    date_col: str,
    start_freq: str,
    end_freq: str
) -> DataFrame:
    """
    Orchestrates preprocessing & aggregation using metadata-driven rules.
    """

    _validate_groupby_cols(raw_df, groupby_cols)
   

    raw_df = execute_date_time_normalization(
        df=raw_df,
        date_col=date_col,
        start_freq=start_freq,
        end_freq=end_freq,
        groupby_cols=groupby_cols
    )
    

    execute_negativity_validation(df=raw_df, metadata_df=metadata_df)
    

    raw_df = execute_flag_normalization(df=raw_df, metadata_df=metadata_df)
    

    # Snapshot column set once — avoids repeated df.columns access inside loop
    raw_df_columns = set(raw_df.columns)

    aggregated_frames = []

    for _, meta_row in metadata_df.iterrows():
        metric_name = meta_row["field_name"]

        if metric_name not in raw_df_columns:
            _log_missing_metric(metric_name)
            continue

        if metric_name in groupby_cols:
            continue

        agg_df = aggregate_metric(
            df=raw_df,
            metric_name=metric_name,
            metric_metadata_row=meta_row,
            metadata_df=metadata_df,
            groupby_cols=groupby_cols
        )

        aggregated_frames.append(agg_df)

    if not aggregated_frames:
        raise ValueError("[PREPROCESS][ERROR] No metrics were aggregated")

    df_aggregated = _merge_aggregated_frames(aggregated_frames, groupby_cols)

    # Null check on final merged frame — one aggregation pass across all
    # non-key columns. Replicates: df.isna().sum().sum() != 0
    value_cols = [c for c in df_aggregated.columns if c not in groupby_cols]
    null_counts = df_aggregated.agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in value_cols]
    ).collect()[0].asDict()

    if any(count > 0 for count in null_counts.values()):
        raise ValueError("[PREPROCESS][ERROR] Missing Values Exist in the final frame")

    
    return df_aggregated


# ------------------------------------------------------------------
# For Outlier Detection and Treatment
# ------------------------------------------------------------------

def run_outlier_post_aggregation(
    df: DataFrame,
    metadata_df,               # Pandas DataFrame — driver-side metadata
    groupby_cols: list,
    date_col: str              # required by execute_outlier_treatment for window ordering
) -> DataFrame:
    

    df = execute_outlier_treatment(
        df=df,
        metadata_df=metadata_df,
        groupby_cols=groupby_cols,
        date_col=date_col
    )

    
    return df


# ------------------------------------------------------------------
# For Missing Value Imputation
# ------------------------------------------------------------------

def run_missing_value_post_outlier(
    df: DataFrame,
    metadata_df,               # Pandas DataFrame — driver-side metadata
    groupby_cols: list,
    date_col: str              # required by execute_missing_value_treatment for window ordering
) -> DataFrame:
    """
    Run missing value treatment after aggregation and outlier correction.
    """
    

    df = execute_missing_value_treatment(
        df=df,
        metadata_df=metadata_df,
        groupby_cols=groupby_cols,
        date_col=date_col
    )

    
    return df


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _merge_aggregated_frames(dfs: list, groupby_cols: list) -> DataFrame:
    """
    Merge aggregated metric Spark DataFrames on groupby columns.
    Each frame contains groupby_cols + one metric column.
    Replicates the Pandas merge(..., how='outer') chain.
    """
    result = dfs[0]

    for df in dfs[1:]:
        result = result.join(df, on=groupby_cols, how="outer")

    return result


def _validate_groupby_cols(df: DataFrame, groupby_cols: list) -> None:
    missing = set(groupby_cols) - set(df.columns)
    if missing:
        raise ValueError(
            f"[PREPROCESS][ERROR] Missing groupby columns in raw data: {missing}"
        )


def _log_missing_metric(metric_name: str) -> None:
    print(
        f"[PREPROCESS][WARN] Metric '{metric_name}' present in metadata "
        f"but missing in raw data. Skipping."
    )