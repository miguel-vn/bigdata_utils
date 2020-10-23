from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window


def get_filtered_by_week(data: DataFrame) -> DataFrame:
    """
    Method transforms periods "start_date - end_date" to year and week number of year

    source:
    +---+----------+----------+
    |key|start_date|  end_date|
    +---+----------+----------+
    |  5|2018-01-01|2018-01-09|
    +---+----------+----------+

    result:
    +---+----------+----------+
    |key|      year|  week_num|
    +---+----------+----------+
    |  5|      2018|         1|
    |  5|      2018|         2|
    +---+----------+----------+
    """

    max_week_number = 53
    transformed_data = data \
        .withColumn('start_week', F.weekofyear('start_date')) \
        .withColumn('weeks_diff', F.ceil(F.datediff(F.col('end_date'),
                                                    F.col('start_date')) / 7)) \
        .withColumn("year", F.year("start_date")) \
        .withColumn("repeat", F.expr("split(repeat(',', weeks_diff), ',')")) \
        .select("*", F.posexplode("repeat").alias("week_add", "val")) \
        .withColumn('total_week_num', F.col('start_week') + F.col('week_add')) \
        .withColumn('add_year', (F.col('total_week_num') / max_week_number).cast(IntegerType())) \
        .withColumn('total_week_num', F.col('total_week_num') - (max_week_number * F.col('add_year'))) \
        .withColumn('week_num',
                    F.when(F.col('total_week_num') == 0, 1)
                    .otherwise(F.col('total_week_num'))) \
        .withColumn('year', F.col('year') + F.col('add_year')) \
        .drop('start_date', 'end_date', 'week_add', 'repeat',
              'val', 'date', 'add_year', 'weeks_diff', 'total_week_num') \
        .dropDuplicates()

    return transformed_data


def get_filtered_by_month(data: DataFrame) -> DataFrame:
    """
    Method transforms periods "start_date - end_date" to year and month number

    source:
    +---+----------+----------+
    |key|start_date|  end_date|
    +---+----------+----------+
    |  5|2018-01-01|2018-01-09|
    +---+----------+----------+

    result:
    +---+----------+----------+
    |key|      year|     month|
    +---+----------+----------+
    |  5|      2018|         1|
    +---+----------+----------+
    """

    transformed_data = data \
        .withColumn("start_date", F.trunc("start_date", "month")) \
        .withColumn("monthsDiff", F.months_between("end_date", "start_date")) \
        .withColumn("repeat", F.expr("split(repeat(',', monthsDiff), ',')")) \
        .select("*", F.posexplode("repeat").alias("date", "val")) \
        .withColumn("month", F.expr("add_months(datab, date)")) \
        .drop('start_date', 'end_date', 'monthsDiff', 'repeat', 'val', 'date') \
        .dropDuplicates()

    return transformed_data


def clear_periods_intersections(source: DataFrame, columns_subset: List[str]) -> DataFrame:
    """
    The method will transform periods "start_date - end_date" for subset of columns
    and change periods for linear continuations

    source:
    +---+----------+----------+
    |key|start_date|  end_date|
    +---+----------+----------+
    |  5|2018-07-01|2018-09-01|
    |  5|2018-08-15|2018-09-13|
    |  5|2018-07-20|2018-08-30|
    +---+----------+----------+

    result:
    +---+----------+----------+
    |key|start_date|  end_date|
    +---+----------+----------+
    |  5|2018-07-01|2018-09-13|
    +---+----------+----------+

    :param source: source data frame.
    :param columns_subset: subset of data frame columns that need to have not periods intersections
    """

    custom_window = Window.partitionBy(*columns_subset).orderBy('start_dt')

    result = source \
        .withColumn('lag_end', F.lag('end_date').over(custom_window)) \
        .withColumn('flg',
                    F.when(F.col('lag_end') < F.date_sub(F.col('start_date'), 1), 1)
                    .otherwise(0)) \
        .withColumn('grp', F.sum('flg').over(custom_window)) \
        .groupBy(*columns_subset, 'grp') \
        .agg(F.min('start_date').alias('start_date'),
             F.max('end_date').alias('end_date')) \
        .drop('grp')

    return result
