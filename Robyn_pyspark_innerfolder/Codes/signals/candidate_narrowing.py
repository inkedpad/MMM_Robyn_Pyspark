import re
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from signals.priority_list import (
    CHANNEL_EXPOSURE_PRIORITY,
    CHANNEL_RESPONSE_PRIORITY,
    DEFAULT_EXPOSURE_PRIORITY,
    DEFAULT_RESPONSE_PRIORITY,
    CONTROL_KEYS,
)


# ==========================================================
# Config
# ==========================================================

MAX_GLOBAL   = 1
MAX_EXPOSURE = 2
MAX_RESPONSE = 4

MAX_NAN_PCT    = 0.30
MIN_CV         = 0.05
MIN_NONZERO    = 0.20
MIN_COVERAGE   = 0.60


# ==========================================================
# Global KPI Priority
# ==========================================================

GLOBAL_RESPONSE_PRIORITY = [
    ["revenue", "sales", "sales_value", "gmv", "turnover"],
    ["qty", "quantity", "unit", "units", "volume", "sold"],
    ["order", "orders", "transaction"],
    ["conversion", "install", "lead"],
]


# ==========================================================
# Public API
# ==========================================================

def execute_candidate_narrowing(
    df: DataFrame,
    metadata_df           # Pandas DataFrame — driver-side metadata
) -> dict:

    print("\n[CANDIDATE] Starting candidate narrowing...")

    results = {
        "global_response": [],
        "channels": {}
    }

    # total_rows used for quality filtering — single count() action, hoisted here
    # so it is never triggered again inside any loop.
    # Replicates: total_rows = len(df)
    total_rows = df.count()

    # Snapshot column set once — O(1) membership checks in _filter_by_quality
    df_columns = set(df.columns)

    # --------------------------------------------------
    # 1. Global Response
    # --------------------------------------------------

    sales_meta     = metadata_df[
        metadata_df["data_category"].astype(str).str.lower() == "sales"
    ]
    sales_metrics  = sales_meta["field_name"].tolist()

    filtered_global = _filter_by_quality(df, df_columns, sales_metrics, total_rows)
    ranked_global   = _rank_by_keywords(filtered_global, GLOBAL_RESPONSE_PRIORITY)[:MAX_GLOBAL]

    results["global_response"] = ranked_global
    print(f"[CANDIDATE] Global response: {ranked_global}")

    # --------------------------------------------------
    # 1.5 Global Controls (Price / Promo / Macro)
    # --------------------------------------------------

    control_meta    = metadata_df[
        metadata_df["data_category"].astype(str).str.lower().isin(["price", "promo", "macro"])
    ]
    control_metrics = control_meta["field_name"].tolist()

    filtered_controls = _filter_by_quality(df, df_columns, control_metrics, total_rows)
    ranked_controls   = filtered_controls.copy()

    if not ranked_controls and control_metrics:
        ranked_controls = control_metrics.copy()
        print("[WARN] Using raw controls (no quality pass)")

    results["global_controls"] = ranked_controls
    print(f"[CANDIDATE] Global controls: {ranked_controls}")

    # --------------------------------------------------
    # 2. Channel-wise Selection
    # --------------------------------------------------

    channel_meta = metadata_df[
        metadata_df["channel"].notna()
        & (~metadata_df["channel"].astype(str).str.lower().str.strip().isin(["nan", "na", '"na"']))
    ]

    if channel_meta.empty:
        raise ValueError("[CANDIDATE] No channel metadata found")

    for channel, meta_ch in channel_meta.groupby("channel"):

        print(f"\n[CANDIDATE] Processing channel: {channel}")

        exposure_fields = []
        response_fields = []
        spend_fields    = []

        # ------------------------------------------
        # Field Classification
        # ------------------------------------------

        for _, row in meta_ch.iterrows():

            field    = row["field_name"]
            dtype    = str(row["data_type"]).lower()
            category = str(row["data_category"]).lower()
            name     = str(field).lower()

            if any(k in name for k in CONTROL_KEYS):
                continue

            if category == "spend":
                spend_fields.append(field)
                continue

            is_grp_trp = any(k in name for k in ["grp", "trp"])

            if dtype == "absolute":
                exposure_fields.append(field)
                continue

            if dtype == "rate" and is_grp_trp:
                exposure_fields.append(field)
                continue

            response_fields.append(field)

        print(f"[DEBUG] Raw exposure: {exposure_fields}")
        print(f"[DEBUG] Raw response: {response_fields}")

        # --------------------------------------------------
        # Exposure Selection
        # --------------------------------------------------

        filtered_exp = _filter_by_quality(
            df, df_columns, exposure_fields, total_rows, skip_cv_for_grp=True
        )

        exp_priority = _get_channel_priority(channel, mode="exposure")
        ranked_exp   = _rank_by_keywords(filtered_exp, exp_priority)[:MAX_EXPOSURE]

        if not ranked_exp and spend_fields:
            ranked_exp = spend_fields[:1]
            print(f"[WARN] {channel}: Using spend as exposure fallback")

        # --------------------------------------------------
        # Spend Selection
        # --------------------------------------------------

        filtered_spend = _filter_by_quality(df, df_columns, spend_fields, total_rows)
        ranked_spend   = filtered_spend[:2]

        if not ranked_spend and spend_fields:
            ranked_spend = spend_fields[:1]
            print(f"[WARN] {channel}: Using raw spend (no quality pass)")

        # --------------------------------------------------
        # Response Selection
        # --------------------------------------------------

        filtered_resp = _filter_by_quality(df, df_columns, response_fields, total_rows)

        tier1, tier2, tier3 = _split_response_tiers(filtered_resp, metadata_df)

        if tier1:
            selected  = tier1
            tier_used = "raw_absolute"
        elif tier2:
            selected  = tier2
            tier_used = "raw_engagement"
        elif tier3:
            selected  = tier3
            tier_used = "derived"
        else:
            selected  = []
            tier_used = "none"

        if not selected and ranked_exp:
            selected  = ranked_exp.copy()
            tier_used = "proxy_exposure"
            print(f"[WARN] {channel}: Using exposure as response proxy")

        resp_priority = _get_channel_priority(channel, mode="response")
        ranked_resp   = _rank_by_keywords(selected, resp_priority)[:MAX_RESPONSE]

        if not ranked_exp:
            print(f"[WARN] {channel}: No exposure found")
        if not ranked_resp:
            print(f"[WARN] {channel}: No response found")

        results["channels"][channel] = {
            "exposure":       ranked_exp,
            "response":       ranked_resp,
            "spend":          ranked_spend,
            "response_tier":  tier_used,
        }

        print(
            f"[CANDIDATE] {channel} | "
            f"Exposure={ranked_exp} | "
            f"Response={ranked_resp} | "
            f"Spend={ranked_spend} | "
            f"Tier={tier_used}"
        )

    print("\n[CANDIDATE] Candidate narrowing complete\n")
    return results


# ==========================================================
# Helpers
# ==========================================================

def _normalize_channel_name(name: str) -> str:
    if not name:
        return ""
    name = str(name).lower()
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def _get_channel_priority(channel_name: str, mode: str = "exposure"):
    ch = _normalize_channel_name(channel_name)

    if mode == "exposure":
        priority_map = CHANNEL_EXPOSURE_PRIORITY
        default      = DEFAULT_EXPOSURE_PRIORITY
    else:
        priority_map = CHANNEL_RESPONSE_PRIORITY
        default      = DEFAULT_RESPONSE_PRIORITY

    for key, priorities in priority_map.items():
        if _normalize_channel_name(key) in ch:
            return priorities

    return default


def _filter_by_quality(
    df: DataFrame,
    df_columns: set,
    metrics: list,
    total_rows: int,
    skip_cv_for_grp: bool = False
) -> list:
    """
    Filters a list of metric column names by quality thresholds.

    In the original Pandas version, stats were computed per column
    individually inside the loop (one pass per column). In PySpark,
    every stat computation would be a separate Spark action if done
    that way. Instead, we compute all four stats for all candidate
    columns in a SINGLE .agg() call — one Spark job regardless of
    how many columns are being evaluated. The results are collected
    to the driver as a plain Python dict, and all filtering logic
    runs in pure Python.
    """

    # Only process columns that actually exist in the DataFrame
    valid_metrics = [col for col in metrics if col in df_columns]

    if not valid_metrics:
        return []

    # ── Single aggregation pass over all valid candidate columns ──────────────
    # Computes nan_pct, non_zero_rate, mean, and stddev per column in one job.
    agg_exprs = []
    for col in valid_metrics:
        c = F.col(col)
        agg_exprs += [
            F.mean(c.isNull().cast("double")).alias(f"__nan__{col}"),
            F.mean((c != 0).cast("double")).alias(f"__nonzero__{col}"),
            F.mean(c.cast("double")).alias(f"__mean__{col}"),
            F.stddev(c.cast("double")).alias(f"__std__{col}"),
        ]

    stats_row = df.agg(*agg_exprs).collect()[0].asDict()

    # ── Driver-side filtering — pure Python from here ─────────────────────────
    usable = []

    for col in valid_metrics:
        name     = col.lower()
        nan_pct  = stats_row[f"__nan__{col}"]  or 0.0
        non_zero = stats_row[f"__nonzero__{col}"] or 0.0
        mean     = stats_row[f"__mean__{col}"] or 0.0
        std      = stats_row[f"__std__{col}"]  or 0.0

        coverage = 1 - nan_pct
        cv       = std / mean if mean != 0 else 0.0

        # Hard filters
        if nan_pct > MAX_NAN_PCT:
            continue

        if std < 1e-6:
            continue

        if skip_cv_for_grp:
            if not any(k in name for k in ["grp", "trp"]):
                if cv < MIN_CV:
                    continue
        else:
            if cv < MIN_CV:
                continue

        # Soft warnings
        if non_zero < MIN_NONZERO:
            print(f"[WARN] {col}: Low density")

        if coverage < MIN_COVERAGE:
            print(f"[WARN] {col}: Low coverage")

        usable.append(col)

    return usable


def _split_response_tiers(
    fields: list,
    metadata_df        # Pandas DataFrame — driver-side
) -> tuple:
    """
    Pure Python + Pandas driver-side logic. No DataFrame involvement.
    Unchanged from original.
    """

    tier1 = []
    tier2 = []
    tier3 = []

    for f in fields:

        row = metadata_df.loc[metadata_df["field_name"] == f]

        if row.empty:
            continue

        is_derived = bool(row.iloc[0].get("is_derived", False))
        dtype      = str(row.iloc[0]["data_type"]).lower()
        name       = f.lower()

        if is_derived:
            tier3.append(f)
            continue

        if dtype == "absolute" and any(
            k in name for k in
            ["order", "conversion", "install", "lead", "purchase", "sale"]
        ):
            tier1.append(f)

        elif dtype == "absolute" and any(
            k in name for k in
            ["click", "engage", "interaction", "pdp", "view"]
        ):
            tier2.append(f)

        else:
            tier3.append(f)

    return tier1, tier2, tier3


def _rank_by_keywords(metrics: list, keyword_groups: list) -> list:
    """Pure Python. Unchanged from original."""

    ranked        = []
    metrics_lower = {m: m.lower() for m in metrics}

    for group in keyword_groups:
        for key in group:
            for original, lowered in metrics_lower.items():
                if key in lowered and original not in ranked:
                    ranked.append(original)

    for m in metrics:
        if m not in ranked:
            ranked.append(m)

    return ranked