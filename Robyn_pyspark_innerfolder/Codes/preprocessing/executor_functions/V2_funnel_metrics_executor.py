import re
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from preprocessing.functions.funnel_metrics import infer_funnel_role


# --------------------------------------------------
# Allowed adjacent funnel combinations
# --------------------------------------------------

VALID_RATIOS = [
    ("engagement", "exposure"),    # CTR
    ("conversion", "engagement"),  # CVR
    ("cost",       "engagement"),  # CPC
    ("cost",       "exposure"),    # CPM
    ("cost",       "conversion")   # CPA
]

# Exposure preference (pick ONE per channel)
EXPOSURE_PRIORITY = ["impressions", "reach", "grp"]


# --------------------------------------------------
# Public API
# --------------------------------------------------

def execute_funnel_metrics(
    df: DataFrame,
    metadata_df          # Pandas DataFrame — small metadata table, driver-side
) -> tuple:             # tuple[DataFrame, pd.DataFrame]
    """
    Create deterministic, channel-aware funnel metrics without
    overwriting or dropping raw metrics.

    `metadata_df` is and remains a Pandas DataFrame throughout. All
    orchestration — role inference, ratio eligibility, column name
    generation, metadata accumulation — is driver-side logic operating
    on a small config table. Only the actual ratio column computation
    touches the Spark DataFrame.
    """

    new_metadata_rows = []
    df_columns        = set(df.columns)  # snapshot once; updated as new cols are added

    for channel, meta_ch in metadata_df.groupby("channel"):

        channel_key  = _normalize_channel_key(channel)
        role_map     = {}   # field_name → role
        ratio_eligible = set()

        # ------------------------------------------
        # Infer funnel roles for this channel
        # ------------------------------------------

        for _, row in meta_ch.iterrows():

            dtype = row["data_type"].lower()

            # Only absolute metrics can participate in ratios
            if dtype == "absolute":
                ratio_eligible.add(row["field_name"])

            role = infer_funnel_role(
                field_name=row["field_name"],
                data_category=row["data_category"],
                data_type=dtype
            )

            # Only add to role_map if a role was inferred AND this metric
            # has not already been assigned a funnel_role in metadata.
            # pd.isna(None) == True, so None is used in place of np.nan.
            if role and pd.isna(row.get("funnel_role", None)):
                role_map[row["field_name"]] = role

                # Persist funnel role back into the driver-side metadata table
                metadata_df.loc[
                    metadata_df["field_name"] == row["field_name"],
                    "funnel_role"
                ] = role

        # ------------------------------------------
        # Pick ONE primary exposure metric
        # ------------------------------------------

        primary_exposure = _select_primary_exposure(role_map, ratio_eligible)

        # ------------------------------------------
        # Generate valid funnel ratio columns
        # ------------------------------------------

        for num_col, num_role in role_map.items():
            for den_col, den_role in role_map.items():

                if (num_role, den_role) not in VALID_RATIOS:
                    continue

                if num_col == den_col:
                    continue

                if num_col not in df_columns or den_col not in df_columns:
                    continue

                # Enforce single exposure metric per channel
                if den_role == "exposure" and den_col != primary_exposure:
                    continue

                # Absolute-only guard
                if num_col not in ratio_eligible or den_col not in ratio_eligible:
                    continue

                num_clean = clean_metric_name(num_col, channel)
                den_clean = clean_metric_name(den_col, channel)
                new_col   = f"{channel_key}__{num_clean}_per_{den_clean}"

                if new_col in df_columns:
                    continue

                # ── Spark transformation: safe division ────────────────────────
                # Replicates: np.where(denom != 0, num / denom, 0.0)
                # when denom == 0, result is 0.0 (matching original behaviour)
                df = df.withColumn(
                    new_col,
                    F.when(
                        F.col(den_col) != 0,
                        F.col(num_col) / F.col(den_col)
                    ).otherwise(F.lit(0.0))
                )

                # Track the new column so subsequent iterations can detect it
                df_columns.add(new_col)

                # ── Logging: pct null in new column ───────────────────────────
                # Single .agg() action per derived column — logging only.
                pct_null_row = df.agg(
                    (F.sum(F.col(new_col).isNull().cast("int")) / F.count("*")).alias("pct")
                ).collect()[0]
                pct_nan = round(100 * (pct_null_row["pct"] or 0.0), 2)

                print(
                    f"[FUNNEL] {new_col} | "
                    f"({num_role}/{den_role}) | NaN%={pct_nan}"
                )

                new_metadata_rows.append({
                    "field_name":     new_col,
                    "channel":        channel,
                    "data_type":      "rate",
                    "data_category":  "response",
                    "can_be_negative": "No",
                    "is_derived":     True,
                    "derived_from":   f"{num_col}/{den_col}",
                    "funnel_role":    f"{num_role}/{den_role}"
                })

    # --------------------------------------------------
    # Append new derived metric rows to metadata
    # --------------------------------------------------

    if new_metadata_rows:
        meta_new = pd.DataFrame(new_metadata_rows)

        existing = set(metadata_df["field_name"])
        meta_new = meta_new[~meta_new["field_name"].isin(existing)]

        metadata_df = pd.concat(
            [metadata_df, meta_new],
            ignore_index=True
        )

    return df, metadata_df


# --------------------------------------------------
# Helper functions — all pure Python, driver-side
# --------------------------------------------------

def _normalize_channel_key(channel: str) -> str:
    return channel.lower().replace(" ", "_")


def _get_channel_tokens(channel: str) -> set:
    base = channel.lower().strip()
    return {
        base,
        base.replace(" ", "_"),
        base.replace("_", " "),
        base.replace(" ", "")
    }


def clean_metric_name(metric_name: str, channel: str) -> str:
    """
    Remove channel identifiers from metric name (prefix, suffix, embedded).
    """
    name   = metric_name.lower()
    tokens = _get_channel_tokens(channel)

    for token in tokens:
        if name.startswith(token + "_"):
            name = name[len(token) + 1:]

        if name.endswith("_" + token):
            name = name[:-(len(token) + 1)]

        name = name.replace("_" + token + "_", "_")

    name = re.sub(r"_+", "_", name).strip("_")
    return name


def _select_primary_exposure(role_map: dict, ratio_eligible: set) -> str | None:
    exposure_cols = [
        col for col, role in role_map.items()
        if role == "exposure" and col in ratio_eligible
    ]

    for key in EXPOSURE_PRIORITY:
        for col in exposure_cols:
            if key in col.lower():
                return col

    return exposure_cols[0] if exposure_cols else None


def _is_absolute(metadata_df, num_col: str, den_col: str) -> bool:
    """
    Retained for completeness — currently commented out in the executor
    in favour of the ratio_eligible set guard. metadata_df is Pandas.
    """
    rows = metadata_df.set_index("field_name")
    return (
        rows.loc[num_col, "data_type"].lower() == "absolute"
        and rows.loc[den_col, "data_type"].lower() == "absolute"
    )