from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

YES_VALUES = {"yes", "y", "true", "1"}
NO_VALUES  = {"no",  "n", "false", "0"}

ALLOWED_VALUES = YES_VALUES | NO_VALUES


def normalize_yes_no_column(df: DataFrame, col_name: str) -> DataFrame:
    """
    Convert Yes/No-like values in a column to 1/0.
    Raises ValueError if any unexpected values are found.
    """

    # Normalize: cast to string, strip whitespace, lowercase
    normalized_col = F.trim(F.lower(F.col(col_name).cast("string")))

    # ── Validation ─────────────────────────────────────────────────────────────

    invalid_df = df.filter(~normalized_col.isin(ALLOWED_VALUES))

    if invalid_df.count() > 0:
        bad_values = [
            row[col_name]
            for row in invalid_df.select(col_name).distinct().collect()
        ]
        raise ValueError(
            f"[FLAG VALIDATION ERROR] Invalid values found: {bad_values}"
        )

    # ── Transformation ─────────────────────────────────────────────────────────
    # Map YES_VALUES → 1, everything else (validated NO_VALUES) → 0
    result = df.withColumn(
        col_name,
        F.when(normalized_col.isin(YES_VALUES), 1)
         .otherwise(0)
         .cast(IntegerType())
    )

    return result