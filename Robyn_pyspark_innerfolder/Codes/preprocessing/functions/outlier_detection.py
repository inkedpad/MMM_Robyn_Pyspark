from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


# ── iqr_outlier_mask ───────────────────────────────────────────────────────────
def iqr_outlier_mask(
    df: DataFrame,
    col_name: str,
    partition_cols: list,
    mask_col: str,
    k: float = 1.5,
    quantile_accuracy: int = 1_000_000
) -> DataFrame:
    """
    Adds a boolean column `mask_col` that is True where the value is an
    IQR outlier within its partition, False otherwise.

    """

    window = Window.partitionBy(partition_cols)

    # Compute Q1, Q3, IQR bounds as window columns
    # percentile_approx(col, p, accuracy) — exact enough at 1_000_000
    df = (
        df
        .withColumn("__q1__", F.percentile_approx(F.col(col_name), 0.25, quantile_accuracy).over(window))
        .withColumn("__q3__", F.percentile_approx(F.col(col_name), 0.75, quantile_accuracy).over(window))
    )

    df = (
        df
        .withColumn("__iqr__",   F.col("__q3__") - F.col("__q1__"))
        .withColumn("__lower__", F.col("__q1__") - k * F.col("__iqr__"))
        .withColumn("__upper__", F.col("__q3__") + k * F.col("__iqr__"))
    )

    # Mask: True where value is below lower bound OR above upper bound
    df = df.withColumn(
        mask_col,
        (F.col(col_name) < F.col("__lower__")) | (F.col(col_name) > F.col("__upper__"))
    )

    df = df.drop("__q1__", "__q3__", "__iqr__", "__lower__", "__upper__")

    return df


# ── zscore_outlier_mask ────────────────────────────────────────────────────────
def zscore_outlier_mask(
    df: DataFrame,
    col_name: str,
    partition_cols: list,
    mask_col: str,
    z_thresh: float = 3.0
) -> DataFrame:
    """
    Adds a boolean column `mask_col` that is True where the absolute
    z-score exceeds z_thresh within its partition, False otherwise.

    If std == 0 or std is null within a partition (all values identical,
    or single-row group), the entire partition mask is False —
    matching the original Pandas guard: `if std == 0 or pd.isna(std)`.
    """

    window = Window.partitionBy(partition_cols)

    df = (
        df
        .withColumn("__mean__", F.mean(F.col(col_name)).over(window))
        .withColumn("__std__",  F.stddev(F.col(col_name)).over(window))
    )

    # Guard: if std is null (single-row group) or 0 (constant group),
    # the mask is False for every row in that partition.
    df = df.withColumn(
        mask_col,
        F.when(
            F.col("__std__").isNull() | (F.col("__std__") == F.lit(0.0)),
            F.lit(False)
        ).otherwise(
            (F.abs(F.col(col_name) - F.col("__mean__")) / F.col("__std__")) > z_thresh
        )
    )

    df = df.drop("__mean__", "__std__")

    return df