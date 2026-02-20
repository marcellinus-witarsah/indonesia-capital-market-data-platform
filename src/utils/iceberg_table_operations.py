from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

from src.utils.logger import logger


class IcebergTableOperations:

    def __init__(self, spark):
        self.spark = spark

    def get_partitioned_row_number(
        self, df, partition_cols, order_cols, order: str=["asc", "desc"]
    ):
        """
        Add a row number column to the dataframe based on partitioning and ordering.

        :param self: the IcebergTableOperations instance
        :param df: the dataframe to process
        :param partition_cols: list of partition column names
        :param order_cols: list of ordering column names
        :param order: order type, "asc" or "desc"
        """
        try:
            if order == "asc":
                df = df.withColumn(
                    "row_number",
                    row_number().over(
                        Window.partitionBy(
                            *[col(key) for key in partition_cols]
                        ).orderBy(*[col(key).asc() for key in order_cols])
                    ),
                )
            elif order == "desc":
                df = df.withColumn(
                    "row_number",
                    row_number().over(
                        Window.partitionBy(
                            *[col(key) for key in partition_cols]
                        ).orderBy(*[col(key).desc() for key in order_cols])
                    ),
                )
        except Exception as e:
            logger.error(f"Error in get_partitioned_row_number: {e}")
            raise e
        return df

    def get_latest_record(
        self,
        table_name: str,
        partition_cols: list,
        order_cols: list,
        filter_date_flag: bool = None,
        start_date: str = None,
        end_date: str = None,
    ):
        """
        Get the latest record for each partition in a table.

        :param self: the IcebergTableOperations instance
        :param table_name: name of the table to process
        :param partition_cols: list of partition column names
        :param order_cols: list of ordering column names
        """
        try:
            logger.info(f"Getting latest record from table {table_name}")
            df = self.spark.table(table_name)
            df = self.get_partitioned_row_number(df, partition_cols, order_cols, order="desc")
            if filter_date_flag:
                df = df.filter(col("load_dttm").between(start_date, end_date))
            df = df.filter(col("row_number") == 1)
        except Exception as e:
            logger.error(f"Error in get_latest_record: {e}")
            raise e
        return df
