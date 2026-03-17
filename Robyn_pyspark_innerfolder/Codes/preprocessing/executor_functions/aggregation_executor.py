import re
from pyspark.sql import DataFrame

from preprocessing.functions.aggregation import (
    aggregate_absolute,
    aggregate_simple_average,
    aggregate_weighted_average,
    aggregate_flag,
)


# ------------------------------------------------------------------
# Configuration (centralized business logic)
# ------------------------------------------------------------------

# Order of preference for response metric weights
RESPONSE_WEIGHT_PRIORITY = [
    "impression",
    "reach",
    "engagement",
    "click",
    "view",
    "install",
    "conversion",
    "spend",
]

# Regex patterns to identify weight columns
WEIGHT_REGEX_MAP = {
    "impression": re.compile(r"(impression|impr|imp)",           re.IGNORECASE),
    "reach":      re.compile(r"(reach|unique_reach)",             re.IGNORECASE),
    "engagement": re.compile(r"(engagement|engage|interaction)",  re.IGNORECASE),
    "click":      re.compile(r"(click|clk)",                      re.IGNORECASE),
    "view":       re.compile(r"(view|video_view|vv|watch)",       re.IGNORECASE),
    "install":    re.compile(r"(install|app_install)",            re.IGNORECASE),
    "conversion": re.compile(r"(conversion|conv|order|purchase)", re.IGNORECASE),
    "spend":      re.compile(r"(spend|cost|expense)",             re.IGNORECASE),
}


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def aggregate_metric(
    df: DataFrame,
    metric_name: str,
    metric_metadata_row,       # pyspark.sql.Row — from collected metadata DataFrame
    metadata_df,               # Pandas DataFrame — small metadata table, driver-side
    groupby_cols: list
) -> DataFrame:
    """
    Aggregate a single metric based on metadata-driven rules.

    `metric_metadata_row` is expected to be a pyspark.sql.Row collected
    from the metadata Spark DataFrame. Row supports dict-style field
    access identically to pd.Series, so the dispatch logic is unchanged.

    `metadata_df` is the metadata table as a Pandas DataFrame (collected
    to the driver before calling this function). It is only used by
    _resolve_response_weight, which performs pure driver-side string
    matching and produces a single column-name string — there is no
    reason to distribute this logic.
    """

    data_type     = metric_metadata_row["data_type"].lower()
    data_category = metric_metadata_row["data_category"].lower()
    channel       = metric_metadata_row["channel"] if "channel" in metric_metadata_row else None

    # Absolute metrics
    if data_type == "absolute":
        _log(metric_name, "sum", None)
        return aggregate_absolute(df, groupby_cols, metric_name)

    # Rate / Percentage metrics
    if data_type in ("rate", "percentage"):
        if data_category == "response":
            weight_col = _resolve_response_weight(
                channel=channel,
                metadata_df=metadata_df,
                df_columns=df.columns
            )

            if weight_col:
                _log(metric_name, "weighted_avg", weight_col)
                return aggregate_weighted_average(df, groupby_cols, metric_name, weight_col)

            _log(metric_name, "mean_fallback", None)
            return aggregate_simple_average(df, metric_name, groupby_cols)

        # Non-response rate/percentage
        _log(metric_name, "mean", None)
        return aggregate_simple_average(df, groupby_cols, metric_name)

    # Flag / Binary metrics
    if data_type in ("flag", "binary"):
        _log(metric_name, "max", None)
        return aggregate_flag(df, groupby_cols, metric_name)

    # Unknown / new data types
    _log_unknown_data_type(
        metric_name=metric_name,
        data_type=data_type,
        data_category=data_category,
    )

    # Safe fallback to simple average (unreachable — _log_unknown_data_type raises)
    return aggregate_simple_average(df, groupby_cols, metric_name)


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _resolve_response_weight(
    channel: str,
    metadata_df,       # Pandas DataFrame — small metadata table, driver-side
    df_columns         # list or pyspark DataFrame.columns
) -> str | None:
    """
    Resolve weight column for response metrics using:
    1. Channel filtering
    2. Regex-based identification
    3. Priority hierarchy

    Operates entirely on the driver using the collected metadata Pandas
    DataFrame. Produces a single column-name string — no Spark ops needed.
    """

    if not channel:
        return None

    channel_metadata = metadata_df[
        (metadata_df["channel"] == channel) &
        (metadata_df["data_type"] == "absolute")
    ]

    candidate_columns = channel_metadata["field_name"].tolist()

    for weight_type in RESPONSE_WEIGHT_PRIORITY:
        pattern = WEIGHT_REGEX_MAP[weight_type]

        for col in candidate_columns:
            if col in df_columns and pattern.search(col):
                return col

    return None


def _log(metric_name: str, strategy: str, weight_col) -> None:
    msg = f"[AGG] {metric_name} | {strategy}"
    if weight_col:
        msg += f" | weight={weight_col}"
    print(msg)


def _log_unknown_data_type(metric_name: str, data_type: str, data_category: str) -> None:
    msg = (
        f"[AGG][WARN] Unknown data_type='{data_type}' "
        f"for metric='{metric_name}' "
        f"(category='{data_category}'). "
    )
    raise ValueError("Unknown data_type in metadata")
