from pyspark.sql import DataFrame

from preprocessing.functions.V2_date_time import (
    normalize_frequency,
    convert_date_grain,
    enforce_date_continuity,
)


def execute_date_time_normalization(
    df: DataFrame,
    date_col: str,
    start_freq: str,
    end_freq: str,
    groupby_cols: list
) -> DataFrame:

    start_freq_norm = normalize_frequency(start_freq)
    end_freq_norm   = normalize_frequency(end_freq)

    # Validate direction — downsampling (e.g. monthly → daily) is not allowed
    order = {"D": 1, "W-SUN": 2, "M": 3, "Q": 4}
    if order[end_freq_norm] < order[start_freq_norm]:
        raise ValueError(
            f"Cannot aggregate from {start_freq} to {end_freq}"
        )

    print(
        f"[DATE] Converting {date_col}: "
        f"{start_freq_norm} → {end_freq_norm}"
    )

    if start_freq_norm != end_freq_norm:
        df = convert_date_grain(df, date_col, end_freq_norm)

    return df