
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DateType

from preprocessing.functions.missing_values import fill_zero, interpolate_past_only, leave_as_is


def execute_missing_value_treatment(
    df: DataFrame,
    metadata_df,              # Pandas DataFrame — small metadata table, driver-side
    groupby_cols: list,
    date_col: str             # explicit date column for window ordering
) -> DataFrame:
    """
    Missing value treatment post aggregation & outlier correction.

    `groupby_cols` should contain the non-date identity columns only
    (e.g. region, channel). `date_col` is kept separate and used as the
    window order column for all time-series-aware operations (ffill, bfill,
    interpolation). This follows the same pattern as missing_values.py.

    `metadata_df` is and remains a Pandas DataFrame — it is a small
    driver-side config table iterated for dispatch logic only.
    """

    # Partition cols = groupby_cols excluding date_col, for safety
    partition_cols = [c for c in groupby_cols if c != date_col]

    print(f"[MISSING] Grouping by: {partition_cols} (Date excluded for time-series imputation)")

    spend_by_channel = (
        metadata_df[
            (metadata_df["data_category"].str.lower() == "spend") &
            (metadata_df["data_type"].str.lower() == "absolute")
        ]
        .groupby("channel")["field_name"]
        .apply(list)
        .to_dict()
    )

    response_abs_by_channel = (
        metadata_df[
            (metadata_df["data_category"].str.lower() == "response") &
            (metadata_df["data_type"].str.lower() == "absolute")
        ]
        .groupby("channel")["field_name"]
        .apply(list)
        .to_dict()
    )

    response_rate_by_channel = (
        metadata_df[
            (metadata_df["data_category"].str.lower() == "response") &
            (metadata_df["data_type"].str.lower().isin(["rate", "percentage"]))
        ]
        .groupby("channel")["field_name"]
        .apply(list)
        .to_dict()
    )

    price_metrics = metadata_df[
        metadata_df["data_category"].str.lower() == "price"
    ]["field_name"].tolist()

    macro_metrics = metadata_df[
        metadata_df["data_category"].str.lower().isin([
            "macro", "macro-economic", "macro economic",
            "macro-economic factors", "external factors", "external"
        ])
    ]["field_name"].tolist()

    flag_metrics = metadata_df[
        metadata_df["data_type"].str.lower().isin(["flag", "binary"])
    ]["field_name"].tolist()

    sales_metrics = metadata_df[
        metadata_df["data_category"].str.lower().isin(["sales", "sale", "revenue"])
    ]["field_name"].tolist()

    df_columns = set(df.columns)  # snapshot once for cheap membership checks

    # --------------------------------------------------
    # 1. SALES → fill 0 (group-wise)
    # --------------------------------------------------
    for metric in sales_metrics:
        if metric not in df_columns:
            continue

        before = _null_count(df, metric)
        df     = fill_zero(df, metric)
        after  = _null_count(df, metric) if before > 0 else 0
        _log(metric, "sales", before, after, "fill_0")

    # --------------------------------------------------
    # 2. FLAG / BINARY → fill 0 (global, no grouping)
    # --------------------------------------------------
    for metric in flag_metrics:
        if metric not in df_columns:
            continue

        before = _null_count(df, metric)
        df     = fill_zero(df, metric)
        after  = _null_count(df, metric) if before > 0 else 0
        _log(metric, "flag", before, after, "fill_0")

    # --------------------------------------------------
    # 3. PRICE → forward fill THEN back fill (group-wise)
    # --------------------------------------------------
    ffill_window = (
        Window
        .partitionBy(partition_cols)
        .orderBy(date_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    bfill_window = (
        Window
        .partitionBy(partition_cols)
        .orderBy(date_col)
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )

    for metric in price_metrics:
        if metric not in df_columns:
            continue

        before = _null_count(df, metric)

        # Step 1: forward fill into a temp column
        df = df.withColumn(
            "__ffilled__",
            F.last(F.col(metric), ignorenulls=True).over(ffill_window)
        )
        # Step 2: back fill the temp column → result goes into metric column
        df = df.withColumn(
            metric,
            F.first(F.col("__ffilled__"), ignorenulls=True).over(bfill_window)
        )
        df = df.drop("__ffilled__")

        after = _null_count(df, metric) if before > 0 else 0
        _log(metric, "price", before, after, "ffill+bfill")

    # --------------------------------------------------
    # 4. MACRO → interpolate past-only (group-wise)
    # --------------------------------------------------
    for metric in macro_metrics:
        if metric not in df_columns:
            continue

        before = _null_count(df, metric)
        df     = interpolate_past_only(df, metric, partition_cols, date_col)
        after  = _null_count(df, metric) if before > 0 else 0
        _log(metric, "macro", before, after, "interpolate")

    # --------------------------------------------------
    # 5. RESPONSE – ABSOLUTE (channel-aware)
    # New version: response > 0 while spend == 0 prints a WARN (no longer raises)
    # --------------------------------------------------
    for channel, resp_metrics in response_abs_by_channel.items():
        spend_metrics_ch = spend_by_channel.get(channel, [])

        for resp in resp_metrics:
            if resp not in df_columns:
                continue

            before = _null_count(df, resp)

            # Validation: warn if response > 0 while spend == 0 (same channel)
            for spend in spend_metrics_ch:
                if spend not in df_columns:
                    continue

                invalid_count = df.filter(
                    (F.col(resp) > 0) & (F.col(spend) == 0)
                ).count()

                if invalid_count > 0:
                    print(f"[WARN] Response '{resp}' > 0 while Spend '{spend}' == 0")

            # Fill missing response with 0
            df    = df.withColumn(metric, F.coalesce(F.col(resp), F.lit(0)))
            after = _null_count(df, resp) if before > 0 else 0
            _log(resp, f"response_abs:{channel}", before, after, "fill_0")

    # --------------------------------------------------
    # 6. SPEND → CONDITIONAL FILL
    # --------------------------------------------------
    for channel, spend_metrics_ch in spend_by_channel.items():
        resp_metrics_ch = response_abs_by_channel.get(channel, [])

        for spend in spend_metrics_ch:
            if spend not in df_columns:
                continue

            before = _null_count(df, spend)

            # Raise if spend is null while any response > 0 for the same channel
            for resp in resp_metrics_ch:
                if resp not in df_columns:
                    continue

                invalid_count = df.filter(
                    F.col(spend).isNull() & (F.col(resp) > 0)
                ).count()

                if invalid_count > 0:
                    raise ValueError(
                        f"[MISSING][ERROR] Spend '{spend}' missing "
                        f"while response '{resp}' > 0 (channel='{channel}')"
                    )

            df    = df.withColumn(spend, F.coalesce(F.col(spend), F.lit(0)))
            after = _null_count(df, spend) if before > 0 else 0
            _log(spend, f"spend:{channel}", before, after, "conditional_fill_0")

    # --------------------------------------------------
    # 7. RESPONSE – RATE / PERCENTAGE (conditional)
    # New version: after spend-conditional fill, remaining nulls are also
    # filled with 0 (blanket fill added in this version).
    # --------------------------------------------------
    for channel, rate_metrics in response_rate_by_channel.items():
        spend_metrics_ch = spend_by_channel.get(channel, [])

        for metric in rate_metrics:
            if metric not in df_columns:
                continue

            before = _null_count(df, metric)

            # Where spend == 0 and rate is null → fill rate with 0
            for spend in spend_metrics_ch:
                if spend not in df_columns:
                    continue

                df = df.withColumn(
                    metric,
                    F.when(
                        (F.col(spend) == 0) & F.col(metric).isNull(),
                        F.lit(0)
                    ).otherwise(F.col(metric))
                )

            # Blanket fill: any remaining nulls → 0 (added in this version)
            df    = df.withColumn(metric, F.coalesce(F.col(metric), F.lit(0)))
            after = _null_count(df, metric) if before > 0 else 0
            _log(metric, f"response_rate:{channel}", before, after, "spend_conditional")

    return df


# --------------------------------------------------
# Private helpers
# --------------------------------------------------

def _null_count(df: DataFrame, col_name: str) -> int:
    """Single Spark action to count nulls in a column."""
    return df.filter(F.col(col_name).isNull()).count()


def _log(metric: str, category: str, before: int, after: int, method: str) -> None:
    """Only logs if nulls existed before treatment — matching new version behaviour."""
    if before > 0:
        print(f"[MISSING] {metric} | {category} | {before} -> {after} | {method}")