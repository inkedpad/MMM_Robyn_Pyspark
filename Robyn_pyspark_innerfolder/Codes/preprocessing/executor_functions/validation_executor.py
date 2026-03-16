import pandas as pd
from pyspark.sql import DataFrame

from preprocessing.functions.validation import validate_non_negative


def execute_negativity_validation(
    df: DataFrame,
    metadata_df           # Pandas DataFrame — small metadata table, driver-side
) -> None:
    """
    Validate metrics based on can_be_negative metadata.

    `metadata_df` remains a Pandas DataFrame. All filtering and iteration
    is driver-side config logic. Only `validate_non_negative` touches the
    Spark DataFrame, and only when can_be_negative == 'no'.
    """

    # Driver-side metadata filtering — exclude non-metric categories and fields
    metadata_df = metadata_df[
        ~metadata_df["data_category"].isin(["date", "Date", "Grain", "grain", "categorical", "Categorical"])
    ]
    metadata_df = metadata_df[
        ~metadata_df["field_name"].isin(["Date", "date"])
    ]

    df_columns = set(df.columns)  # snapshot once for O(1) membership checks

    for _, row in metadata_df.iterrows():
        metric = row["field_name"]

        if metric not in df_columns:
            continue  # already handled elsewhere

        can_be_negative = row["can_be_negative"]

        # pd.isna() is correct here — can_be_negative is a scalar value
        # from the Pandas metadata table, not a Spark column
        if pd.isna(can_be_negative):
            raise ValueError(
                f"[METADATA][ERROR] can_be_negative missing for metric '{metric}'"
            )

        can_be_negative = str(can_be_negative).strip().lower()

        if can_be_negative not in ("yes", "no", "y", "n"):
            raise ValueError(
                f"[METADATA][ERROR] Invalid can_be_negative='{can_be_negative}' "
                f"for metric '{metric}' (expected Yes/No)"
            )

        if can_be_negative == "no":
            validate_non_negative(df, metric)