import logging

from py4j.protocol import Py4JNetworkError, Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class SparkDataWriter:

    def __init__(self, spark: SparkSession, checkpoint_path: str, log_level='FATAL'):
        self.spark = spark
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel(log_level)

        self.checkpoint_path = checkpoint_path

    def checkpoint(self, df: DataFrame, filename: str) -> DataFrame:
        """
        Метод для сохранения промежуточных результатов работы на HDFS
        :param df: датафрейм, который требуется записать
        :param filename: имя файла записи. Путь к директории сохранения должен быть указан в checkpoint_path

        :return: same DataFrame after writing on HDFS
        """

        try:
            logging.info('Checkpoint %s in HDFS', filename)

            df.write.mode('overwrite').orc(self.checkpoint_path + filename)
            return self.spark.read.orc(self.checkpoint_path + filename)

        except (Py4JNetworkError, Py4JJavaError) as e:
            logging.exception(f'Error through writing {self.checkpoint_path + filename}')
            raise e

    def insert_into_table(self, data_for_insert: DataFrame, table: str, overwrite=False, partition_by=None):
        """
        Method for writing data into hive-table

        :param data_for_insert: DataFrame for writing
        :param table: table name. If not exist, table will be created
        :param overwrite: if True, overwrites existing data
        :param partition_by: partitioning column name

        :return: None
        """

        table_exists = self.__check_table_existence(table=table)

        try:
            logging.info('Writing in Hive (%s)', table)

            if not table_exists:
                data_for_insert.write.saveAsTable(name=table,
                                                  partitionBy=partition_by,
                                                  mode='overwrite' if overwrite else 'append',
                                                  format='orc')
            else:
                data_for_insert.write.insertInto(table, overwrite=overwrite)

        except (Py4JNetworkError, Py4JJavaError):
            logging.exception('Writing error')

    def __check_table_existence(self, table):
        """
        Method for checking table existence
        :param table: full table name (<database>.<table>)
        :return: True or False
        """

        logging.info('Checking table existence.')
        database = table[:table.index('.')]
        tablenames = self.spark.sql(f'show tables in {database}') \
            .withColumn('tableName', F.concat(F.col('database'), F.lit('.'), F.col('tableName'))) \
            .select('tableName') \
            .filter(F.col('tableName') == table) \
            .collect()

        return bool(tablenames)
