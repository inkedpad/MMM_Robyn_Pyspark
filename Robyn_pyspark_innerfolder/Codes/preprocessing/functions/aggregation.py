from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_absolute(df: DataFrame, group_by, value_col: str) -> DataFrame:
    return (
        df
        .groupBy(group_by)
        .agg(F.sum(value_col).alias(value_col))
    )


def aggregate_weighted_average(df: DataFrame, group_by, value_col: str, weight_col: str) -> DataFrame:

    grouped = df.groupBy(group_by)

    weighted_sum = grouped.agg(
        F.sum(F.col(value_col) * F.col(weight_col)).alias('weighted_sum')
    )

    basic_mean = grouped.agg(
        F.mean(value_col).alias('basic_mean')
    )

    weight_sum = grouped.agg(
        F.sum(weight_col).alias('weight_sum')
    )

    result = weighted_sum.join(weight_sum, on=group_by, how='outer')
    result = result.join(basic_mean, on=group_by, how='outer')

    result = result.withColumn(
        value_col,
        F.when(F.col('weight_sum') > 0, F.col('weighted_sum') / F.col('weight_sum'))
         .otherwise(F.col('basic_mean'))
    )

    result = result.drop('weighted_sum', 'weight_sum', 'basic_mean')

    print("WEEIIGGHHHTTT SUMMM:::::::::::")
    result.show()

    return result


def aggregate_simple_average(df: DataFrame, group_by, value_col: str) -> DataFrame:
    return (
        df
        .groupBy(group_by)
        .agg(F.mean(value_col).alias(value_col))
    )


def aggregate_flag(df: DataFrame, group_by, value_col: str) -> DataFrame:
    return (
        df
        .groupBy(group_by)
        .agg(F.max(value_col).alias(value_col))
    )