import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

FREQ_ALIASES = {
    "D":     {"d", "day", "daily"},
    "W-SUN": {"w", "week", "weekly"},
    "M":     {"m", "month", "monthly"},
    "Q":     {"a", "quarter", "quartely", "quart"}
}

FREQ_TO_INTERVAL = {
    "D":     "INTERVAL 1 DAY",
    "W-SUN": "INTERVAL 7 DAYS",
    "M":     "INTERVAL 1 MONTH",
    "Q":     "INTERVAL 3 MONTHS",
}


# ── normalize_frequency ────────────────────────────────────────────────────────
# Pure Python — no DataFrame involved, unchanged.
def normalize_frequency(freq: str) -> str:
    if not isinstance(freq, str):
        raise ValueError("Frequency must be a string")

    freq_clean = freq.strip().lower()

    for canonical, aliases in FREQ_ALIASES.items():
        if freq_clean == canonical.lower() or freq_clean in aliases:
            return canonical

    raise ValueError(
        f"Invalid frequency '{freq}'. "
        f"Allowed values: daily, weekly, monthly"
    )


# ── convert_date_grain ─────────────────────────────────────────────────────────
def convert_date_grain(
    df: DataFrame,
    date_col: str,
    target_freq: str
) -> DataFrame:
    # Replicate format="mixed", dayfirst=True, errors="coerce":
    # Try dd-MM-yyyy first (dayfirst), fall back to yyyy-MM-dd.
    # F.to_date returns null for unparseable values — matching errors="coerce".
    df = df.withColumn(
        date_col,
        F.coalesce(
            F.to_date(F.col(date_col), "dd-MM-yyyy"),
            F.to_date(F.col(date_col), "yyyy-MM-dd")
        )
    )

    if target_freq == "W-SUN":
        # Spark's date_trunc("week") anchors to Monday.
        # Subtract 1 day to get the Sunday start, matching Pandas W-SUN.
        df = df.withColumn(
            date_col,
            F.date_add(F.date_trunc("week", F.col(date_col)).cast(DateType()), -1)
        )

    elif target_freq == "M":
        df = df.withColumn(
            date_col,
            F.date_trunc("month", F.col(date_col)).cast(DateType())
        )

    elif target_freq == "Q":
        df = df.withColumn(
            date_col,
            F.date_trunc("quarter", F.col(date_col)).cast(DateType())
        )

    return df


# ── enforce_date_continuity ────────────────────────────────────────────────────
def enforce_date_continuity(
    df: DataFrame,
    date_col: str,
    freq: str,
    groupby_cols: list
) -> DataFrame:
    """
    Ensures every group has a contiguous sequence of dates from its
    min to its max date at the given frequency. Missing dates are added
    as rows with null values for all non-key columns.

    Note: date_col is intentionally excluded from the partition keys.
    Grouping by the date column itself would create one group per date,
    making the range expansion meaningless. groupby_cols is expected to
    contain only the non-date identity columns (e.g. region, channel).
    """

    if freq not in FREQ_TO_INTERVAL:
        raise ValueError(
            f"Unsupported frequency '{freq}'. "
            f"Allowed: {list(FREQ_TO_INTERVAL.keys())}"
        )

    interval    = FREQ_TO_INTERVAL[freq]
    # Safely exclude date_col in case the caller included it in groupby_cols
    non_date_groups = [c for c in groupby_cols if c != date_col]

    # Ensure date column is DateType before any operations
    df = df.withColumn(date_col, F.col(date_col).cast(DateType()))

    # ── Step 1: Compute min and max date per group ─────────────────────────────
    spine = df.groupBy(non_date_groups).agg(
        F.min(date_col).alias("__min_date__"),
        F.max(date_col).alias("__max_date__")
    )

    # ── Step 2: Generate full date sequence per group using sequence() ─────────
    # F.expr is required here because sequence() with INTERVAL literals
    # must be expressed in Spark SQL syntax — not available via the Column API.
    spine = spine.withColumn(
        "__date_array__",
        F.expr(f"sequence(__min_date__, __max_date__, {interval})")
    )

    # ── Step 3: Explode — one row per (group, date) ────────────────────────────
    spine = (
        spine
        .withColumn(date_col, F.explode(F.col("__date_array__")))
        .drop("__min_date__", "__max_date__", "__date_array__")
    )

    # ── Step 4: Left-join original data onto spine to expose gaps as null rows ──
    # This replicates .reindex(full_range) — dates present in the spine but
    # absent in the original data appear as rows with nulls in all value columns.
    join_keys = non_date_groups + [date_col]
    result = spine.join(df, on=join_keys, how="left")

    return result