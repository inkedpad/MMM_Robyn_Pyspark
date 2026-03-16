import pandas as pd
from pyspark.sql import SparkSession


# ------------------------------------------------------------------
# Allowed metadata values (schema contract)
# ------------------------------------------------------------------

ALLOWED_DATA_TYPES       = {"absolute", "rate", "percentage", "flag", "binary", "date", "categorical"}
ALLOWED_DATA_CATEGORIES  = {"response", "spend", "macro", "grain", "date", "sales", "promo", "price"}


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def load_metric_metadata(file_path: str, spark: SparkSession = None) -> pd.DataFrame:
    """
    Load, clean, and validate metric metadata. Returns a Pandas DataFrame.
    Fails fast on schema issues.

    This function intentionally returns a Pandas DataFrame, not a Spark
    DataFrame. Metadata is a small driver-side config table (dozens to
    hundreds of rows). Every executor in this codebase receives it as
    Pandas and iterates over it with iterrows() for driver-side dispatch
    logic. Converting it to a Spark DataFrame would be architecturally
    wrong and would break all downstream consumers.

    If a SparkSession is provided, Spark is used to read the file — this
    supports distributed file system paths (s3://, hdfs://, abfss://)
    that pd.read_csv cannot handle. The result is immediately brought to
    the driver with .toPandas(). If no SparkSession is provided, falls
    back to pd.read_csv for local paths.
    """

    if spark is not None:
        df_meta = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")   # keep all columns as strings
            .csv(file_path)                   # handles local, HDFS, S3, ADLS paths
            .toPandas()
        )
    else:
        df_meta = pd.read_csv(file_path)

    _validate_required_columns(df_meta)

    # Strip & normalize
    df_meta["field_name"]     = df_meta["field_name"].astype(str).str.strip()
    df_meta["data_type"]      = df_meta["data_type"].astype(str).str.strip().str.lower()
    df_meta["data_category"]  = df_meta["data_category"].astype(str).str.strip().str.lower()
    df_meta["channel"]        = df_meta["channel"].astype(str).str.strip().str.lower()

    _validate_values(df_meta)

    return df_meta.reset_index(drop=True)


# ------------------------------------------------------------------
# Validation helpers
# ------------------------------------------------------------------

def _validate_required_columns(df: pd.DataFrame) -> None:
    required_cols = {"field_name", "data_type", "data_category", "channel", "can_be_negative"}
    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"[METADATA][ERROR] Missing required columns: {missing}")


def _validate_values(df: pd.DataFrame) -> None:
    # Null check on all required columns
    for col in ["field_name", "data_type", "data_category", "channel", "can_be_negative"]:
        if df[col].isna().any():
            raise ValueError(
                f"[METADATA][ERROR] Null values found in required column '{col}'"
            )

    # Invalid 'data_type'
    invalid_types = set(df["data_type"]) - ALLOWED_DATA_TYPES
    if invalid_types:
        raise ValueError(
            f"[METADATA][ERROR] Invalid data_type values: {invalid_types}"
        )

    # Invalid 'data_category'
    invalid_categories = set(df["data_category"]) - ALLOWED_DATA_CATEGORIES
    if invalid_categories:
        raise ValueError(
            f"[METADATA][ERROR] Invalid data_category values: {invalid_categories}"
        )