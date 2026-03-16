from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_non_negative(
    df: DataFrame,
    metric_name: str
) -> None:
    """
    Raise ValueError if any negative values are found in metric_name.

    Uses a single .count() action to both detect and quantify negatives
    in one pass — more efficient than the Pandas version which calls
    .any() and .sum() separately.
    """

    bad_count = df.filter(F.col(metric_name) < 0).count()

    if bad_count > 0:
        raise ValueError(
            f"[VALIDATION][ERROR] Metric '{metric_name}' "
            f"contains {bad_count} negative values but "
            f"metadata specifies can_be_negative = No"
        )