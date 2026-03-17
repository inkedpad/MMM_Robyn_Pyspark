from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from preprocessing.functions.flag_utils import normalize_yes_no_column


def execute_flag_normalization(
    df: DataFrame,
    metadata_df          # Pandas DataFrame — small metadata table, driver-side
) -> DataFrame:
    """
    Validate and normalize flag/binary columns (Yes/No → 1/0).

    `metadata_df` is the metadata table as a Pandas DataFrame, collected
    to the driver before calling this function. The loop iterates over a
    small number of config rows — there is no reason to distribute this.
    """

    df_columns = df.columns  # snapshot column list once, outside the loop

    flag_meta = metadata_df[
        metadata_df["data_type"].str.lower().isin(["flag", "binary"])
    ]

    for _, row in flag_meta.iterrows():
        col = row["field_name"]

        if col not in df_columns:
            continue

        if not isinstance(df.schema[col].dataType, StringType):
            continue

        df = normalize_yes_no_column(df, col)
        print(f"[FLAG] Normalized Yes/No → 1/0 for '{col}'")

    return df