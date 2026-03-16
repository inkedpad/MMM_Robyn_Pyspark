from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


# ── winsorize_series ───────────────────────────────────────────────────────────
def winsorize_series(
    df: DataFrame,
    col_name: str,
    partition_cols: list,
    lower_q: float = 0.01,
    upper_q: float = 0.99,
    quantile_accuracy: int = 1_000_000
) -> DataFrame:
    """
    Clips values in col_name to [lower_q, upper_q] percentile bounds
    computed within each partition. Equivalent to Pandas .clip() after
    group-wise .quantile().
    """

    window = Window.partitionBy(partition_cols)

    df = (
        df
        .withColumn("__lower__", F.percentile_approx(F.col(col_name), lower_q, quantile_accuracy).over(window))
        .withColumn("__upper__", F.percentile_approx(F.col(col_name), upper_q, quantile_accuracy).over(window))
    )

    # F.greatest(col, lower) clips from below; F.least(..., upper) clips from above
    # Together this replicates s.clip(lower=lower, upper=upper) exactly
    df = df.withColumn(
        col_name,
        F.least(
            F.greatest(F.col(col_name), F.col("__lower__")),
            F.col("__upper__")
        )
    )

    df = df.drop("__lower__", "__upper__")

    return df


# ── forward_fill_outliers ──────────────────────────────────────────────────────
def forward_fill_outliers(
    df: DataFrame,
    col_name: str,
    partition_cols: list,
    order_col: str,
    detection: str = "iqr",
    quantile_accuracy: int = 1_000_000,
    z_thresh: float = 3.0
) -> DataFrame:
    """
    Nulls out outlier values in col_name then forward-fills within each
    partition ordered by order_col.

    In the original Pandas code, forward_fill_outliers re-derives its own
    mask internally (see V2 executor line 63). This function mirrors that —
    it detects outliers and forward-fills in one pass, so no mask column
    needs to be pre-computed or passed by the caller.

    detection: 'iqr' or 'zscore' — controls which detection method is used
    to identify outliers before nulling them out.
    """

    part_window  = Window.partitionBy(partition_cols)
    order_window = (
        Window
        .partitionBy(partition_cols)
        .orderBy(order_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # ── Step 1: Derive the outlier mask internally ─────────────────────────────
    if detection == "iqr":
        df = (
            df
            .withColumn("__q1__",    F.percentile_approx(F.col(col_name), 0.25, quantile_accuracy).over(part_window))
            .withColumn("__q3__",    F.percentile_approx(F.col(col_name), 0.75, quantile_accuracy).over(part_window))
            .withColumn("__iqr__",   F.col("__q3__") - F.col("__q1__"))
            .withColumn("__lower__", F.col("__q1__") - 1.5 * F.col("__iqr__"))
            .withColumn("__upper__", F.col("__q3__") + 1.5 * F.col("__iqr__"))
            .withColumn(
                "__mask__",
                (F.col(col_name) < F.col("__lower__")) | (F.col(col_name) > F.col("__upper__"))
            )
            .drop("__q1__", "__q3__", "__iqr__", "__lower__", "__upper__")
        )

    elif detection == "zscore":
        df = (
            df
            .withColumn("__mean__", F.mean(F.col(col_name)).over(part_window))
            .withColumn("__std__",  F.stddev(F.col(col_name)).over(part_window))
            .withColumn(
                "__mask__",
                F.when(
                    F.col("__std__").isNull() | (F.col("__std__") == F.lit(0.0)),
                    F.lit(False)
                ).otherwise(
                    (F.abs(F.col(col_name) - F.col("__mean__")) / F.col("__std__")) > z_thresh
                )
            )
            .drop("__mean__", "__std__")
        )

    else:
        raise ValueError(f"Unsupported detection method '{detection}'. Use 'iqr' or 'zscore'.")

    # ── Step 2: Null out detected outliers ─────────────────────────────────────
    df = df.withColumn(
        "__nulled__",
        F.when(F.col("__mask__"), F.lit(None)).otherwise(F.col(col_name))
    )

    # ── Step 3: Forward-fill within partition, ordered by date ─────────────────
    df = df.withColumn(
        col_name,
        F.last(F.col("__nulled__"), ignorenulls=True).over(order_window)
    )

    df = df.drop("__mask__", "__nulled__")

    return df


# ── smooth_series ──────────────────────────────────────────────────────────────
def smooth_series(
    df: DataFrame,
    col_name: str,
    partition_cols: list,
    order_col: str,
    window_size: int = 3
) -> DataFrame:
    """
    Replaces each value with the rolling mean of the current and preceding
    (window_size - 1) rows within each partition, ordered by order_col.

    min_periods=1 behaviour is automatic: Spark's avg() over a rowsBetween
    window computes the mean of however many rows are actually present in
    the frame, so partial windows at the start of a partition are handled
    correctly without any extra logic.
    """

    rolling_window = (
        Window
        .partitionBy(partition_cols)
        .orderBy(order_col)
        # window_size=3 → current row + 2 preceding rows → rowsBetween(-2, 0)
        .rowsBetween(-(window_size - 1), Window.currentRow)
    )

    df = df.withColumn(
        col_name,
        F.avg(F.col(col_name)).over(rolling_window)
    )

    return df