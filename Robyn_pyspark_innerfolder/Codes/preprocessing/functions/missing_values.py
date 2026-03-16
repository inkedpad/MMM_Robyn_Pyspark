from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import LongType


# ── fill_zero ──────────────────────────────────────────────────────────────────
def fill_zero(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))


# ── forward_fill ───────────────────────────────────────────────────────────────
def forward_fill(df: DataFrame, col_name: str, partition_cols: list, order_col: str) -> DataFrame:
    """
    Within each partition (non-date group cols), ordered by date,
    fill nulls with the most recent preceding non-null value.
    """
    window = (
        Window
        .partitionBy(partition_cols)
        .orderBy(order_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return df.withColumn(
        col_name,
        F.last(F.col(col_name), ignorenulls=True).over(window)
    )


# ── interpolate_past_only ──────────────────────────────────────────────────────
def interpolate_past_only(
    df: DataFrame,
    col_name: str,
    partition_cols: list,
    order_col: str
) -> DataFrame:
    """
    Linear interpolation, forward-only (no extrapolation beyond last known value).

    For each null row:
      - Finds the last known non-null value BEFORE it  (prev_val at prev_pos)
      - Finds the next known non-null value AFTER  it  (next_val at next_pos)
      - Interpolates linearly between them
      - If no next known value exists (gap trails off), leaves as null
        (honouring limit_direction='forward' — no extrapolation)
    """

    # Row number to measure distance between known points
    row_window = Window.partitionBy(partition_cols).orderBy(order_col)

    tmp_pos   = f"__row_pos_{col_name}__"
    tmp_prev  = f"__prev_val_{col_name}__"
    tmp_next  = f"__next_val_{col_name}__"
    tmp_ppos  = f"__prev_pos_{col_name}__"
    tmp_npos  = f"__next_pos_{col_name}__"

    # Assign a sequential row position within each partition
    df = df.withColumn(tmp_pos, F.row_number().over(row_window).cast(LongType()))

    # Forward window: last non-null value and its position seen so far
    fwd_window = (
        Window
        .partitionBy(partition_cols)
        .orderBy(order_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Backward window: next non-null value and its position coming up
    bwd_window = (
        Window
        .partitionBy(partition_cols)
        .orderBy(order_col)
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )

    df = (
        df
        .withColumn(tmp_prev, F.last(F.col(col_name),  ignorenulls=True).over(fwd_window))
        .withColumn(tmp_next, F.first(F.col(col_name), ignorenulls=True).over(bwd_window))
        # Carry forward the row position of the last known value
        .withColumn(
            tmp_ppos,
            F.last(
                F.when(F.col(col_name).isNotNull(), F.col(tmp_pos)),
                ignorenulls=True
            ).over(fwd_window)
        )
        # Carry backward the row position of the next known value
        .withColumn(
            tmp_npos,
            F.first(
                F.when(F.col(col_name).isNotNull(), F.col(tmp_pos)),
                ignorenulls=True
            ).over(bwd_window)
        )
    )

    # Interpolate only for null rows that have BOTH a prev and next known value.
    # Rows with no next known value are left null (no forward extrapolation).
    df = df.withColumn(
        col_name,
        F.when(
            F.col(col_name).isNotNull(),
            F.col(col_name)  # already has a value, keep it
        ).when(
            F.col(tmp_prev).isNotNull() & F.col(tmp_next).isNotNull(),
            # Linear interpolation:
            # prev_val + (next_val - prev_val) * (curr_pos - prev_pos) / (next_pos - prev_pos)
            F.col(tmp_prev) + (
                (F.col(tmp_next) - F.col(tmp_prev)) *
                (F.col(tmp_pos)  - F.col(tmp_ppos)) /
                (F.col(tmp_npos) - F.col(tmp_ppos))
            )
        )
        .otherwise(F.lit(None))  # trailing nulls: no future anchor, leave as null
    )

    # Drop all temporary working columns
    df = df.drop(tmp_pos, tmp_prev, tmp_next, tmp_ppos, tmp_npos)

    return df


# ── leave_as_is ────────────────────────────────────────────────────────────────
def leave_as_is(df: DataFrame, col_name: str) -> DataFrame:
    return df