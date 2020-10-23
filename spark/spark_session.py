import os
from random import randint
from subprocess import check_output

from pyspark.sql import SparkSession


def get_spark_session(name: str, additional_conf: dict, get_optimal=False) -> SparkSession:
    """
    Function creates configured Spark session
    :param name: spark application name
    :param additional_conf: dict of parameters to change default cluster settings
    :param get_optimal: if true it will:
        - get total nodes in your Yarn cluster;
        - set 2 executors per node
        - default parallelism x2 of NumExecutors;
        - shuffle partitions - x4 of NumExecutors.
    :return: configured SparkSession
    """

    environ = additional_conf.pop('environ')
    os.environ['SPARK_HOME'] = environ['spark_home']
    os.environ['PYSPARK_PYTHON'] = environ['pyspark_python']
    os.environ['PATH'] = f"/bin:{os.environ['PATH']}"

    if get_optimal:
        num_nodes_pattern = 'Total Nodes:'
        num_nodes = check_output(f"yarn node -list | grep '{num_nodes_pattern}'",
                                 shell=True) \
            .decode('utf-8') \
            .replace('\n', '') \
            .replace(num_nodes_pattern, '')

        additional_conf["spark.dynamicAllocation.maxExecutors"] = str(int(num_nodes) * 2)
        additional_conf["spark.default.parallelism"] = str(int(num_nodes) * 4)
        additional_conf["spark.sql.shuffle.partitions"] = str(int(num_nodes) * 8)

    spark_session = SparkSession \
        .builder \
        .appName(name) \
        .master('yarn')

    for key, value in additional_conf.items():
        spark_session.config(key=key, value=value)

    spark_session = spark_session.config('spark.ui.port', f'{randint(4040, 4099)}')

    spark_session = spark_session \
        .enableHiveSupport() \
        .getOrCreate()

    return spark_session
