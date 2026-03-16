from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from preprocessing.functions.outlier_detection import iqr_outlier_mask, zscore_outlier_mask
from preprocessing.functions.outlier_treatment import winsorize_series, forward_fill_outliers, smooth_series


EXCLUDED_DTYPES = {"date", "categorical", "flag", "binary"}

OUTLIER_CONFIG = {
    "spend":    {"detection": "iqr",     "treatment": "winsorize"},
    "sales":    {"detection": "iqr",     "treatment": "winsorize"},
    "promo":    {"detection": "iqr",     "treatment": "winsorize"},
    "response": {"detection": "iqr",     "treatment": "winsorize"},
    "price":    {"detection": "iqr",     "treatment": "forward_fill"},
    "macro":    {"detection": "zscore",  "treatment": "smooth"},
}


def execute_outlier_treatment(
    df: DataFrame,
    metadata_df,              # Pandas DataFrame — small metadata table, driver-side
    groupby_cols: list,
    date_col: str             # explicit date column for window ordering
) -> DataFrame:
    """
    Detect and treat outliers per metric, driven by OUTLIER_CONFIG.

    `groupby_cols` should contain non-date identity columns only.
    `date_col` is used as the window order column for forward_fill and
    smooth treatments, consistent with the pattern in outlier_treatment.py.

    `metadata_df` remains a Pandas DataFrame — it is a small driver-side
    config table used only for dispatch logic.
    """

    print("--- Starting Outlier Treatment ---")

    # Partition cols: non-date identity columns for window partitioning
    partition_cols = [c for c in groupby_cols if c != date_col]
    print(f"Grouping by: {partition_cols} (Date excluded)")

    # Filter metadata to only processable types — driver-side, once, outside loop
    valid_metadata = metadata_df[
        ~metadata_df["data_type"].str.lower().isin(EXCLUDED_DTYPES)
    ]

    df_columns = set(df.columns)

    # Compute total row count once outside the loop —
    # avoids triggering a separate count() Spark action per metric
    total_rows = df.count()

    for _, row in valid_metadata.iterrows():
        metric   = row["field_name"]
        category = row["data_category"].lower()

        if metric not in df_columns:
            continue
        if category not in OUTLIER_CONFIG:
            continue

        config   = OUTLIER_CONFIG[category]
        mask_col = f"__mask_{metric}__"

        try:
            # ----------------------------------------------------------
            # Detection: add a boolean mask column marking outlier rows.
            # Used here only for outlier count logging.
            # Treatment functions re-derive their own mask internally.
            # ----------------------------------------------------------
            if config["detection"] == "iqr":
                df = iqr_outlier_mask(
                    df,
                    col_name=metric,
                    partition_cols=partition_cols,
                    mask_col=mask_col
                )
            elif config["detection"] == "zscore":
                df = zscore_outlier_mask(
                    df,
                    col_name=metric,
                    partition_cols=partition_cols,
                    mask_col=mask_col
                )
            else:
                # Unknown detection method — add an all-False mask column
                df = df.withColumn(mask_col, F.lit(False))

            # Outlier count logging — single action using the mask column
            total_outliers = df.filter(F.col(mask_col)).count()

            if total_outliers > 0:
                pct = round(100 * total_outliers / total_rows, 2)
                print(f"[OUTLIER] {metric} | {category} | {total_outliers}/{total_rows} ({pct}%)")

            # Drop mask column — it was only needed for logging
            df = df.drop(mask_col)

            # ----------------------------------------------------------
            # Treatment: applied independently of the mask above.
            # Each treatment function handles its own internal logic.
            # ----------------------------------------------------------
            if config["treatment"] == "winsorize":
                df = winsorize_series(
                    df,
                    col_name=metric,
                    partition_cols=partition_cols
                )

            elif config["treatment"] == "forward_fill":
                # forward_fill_outliers re-derives its mask internally
                # using the detection method specified in config
                df = forward_fill_outliers(
                    df,
                    col_name=metric,
                    partition_cols=partition_cols,
                    order_col=date_col,
                    detection=config["detection"]
                )

            elif config["treatment"] == "smooth":
                df = smooth_series(
                    df,
                    col_name=metric,
                    partition_cols=partition_cols,
                    order_col=date_col
                )

        except Exception as e:
            # Clean up mask column if exception occurred after it was added
            if mask_col in df.columns:
                df = df.drop(mask_col)
            print(f"[ERROR] Failed to process {metric}: {e}")

    print("--- Outlier Treatment Completed ---")
    return df