import argparse
import os
from datetime import datetime

from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import TimestampType

from src.utils.iceberg_table_operations import IcebergTableOperations
from src.utils.logger import logger


def main(start_date, end_date):
    try:
        # Create Spark Session
        spark = SparkSession.builder.appName("silver_pipeline").getOrCreate()
        logger.info("Spark Session created successfully.")

        # Instantiate Iceberg Table Operations
        iceberg_table_ops = IcebergTableOperations(spark)

        # -----------------------------------------------------------
        # Data Load
        # -----------------------------------------------------------
        df = iceberg_table_ops.get_latest_record(
            f"{os.getenv('CATALOG_NAME')}.bronze.market_data_1m",
            ["Ticker", "Datetime"],
            ["load_dttm"],
            True,
            start_date,
            end_date,
        )
        logger.info("Spark table loaded successfully.")

        # -----------------------------------------------------------
        # Data Transformation
        # -----------------------------------------------------------
        df = df.alias("market_data").select(
            col("market_data.Ticker").alias("ticker"),
            col("market_data.Datetime").alias("datetime"),
            col("market_data.Open").alias("open"),
            col("market_data.High").alias("high"),
            col("market_data.Low").alias("low"),
            col("market_data.Close").alias("close"),
            col("market_data.Volume").alias("volume"),
            col("market_data.Dividends").alias("dividends"),
            col("market_data.`Stock Splits`").alias("stock_splits"),
        )
        df = df.withColumn("load_dttm", lit(datetime.now()).cast(TimestampType()))
        logger.info(f"Data transformation completed.")

        # -----------------------------------------------------------
        # Data Upsert
        # -----------------------------------------------------------
        df.createOrReplaceTempView("view_ticker_ohlcv_1m")
        query = f"""
        MERGE INTO {os.getenv('CATALOG_NAME')}.silver.{__file__.split('/')[-1].split('.')[0]} AS target
        USING view_ticker_ohlcv_1m AS source
        ON target.ticker = source.ticker
        AND target.datetime = source.datetime
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """

        # Upsert to Iceberg Table
        spark.sql(query)
        logger.info(f"Spark table upserted successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e


if __name__ == "__main__":
    # Load environment variables
    load_dotenv(find_dotenv())

    # Prepare arguments
    parser = argparse.ArgumentParser(description="Ticker Info Ingestion Pipeline")
    parser.add_argument(
        "--start-date", type=str, help="The start date for market data retrieval"
    )
    parser.add_argument(
        "--end-date", type=str, help="The end date for market data retrieval"
    )
    args = parser.parse_args()

    # Insert arguments
    start_date = args.start_date
    end_date = args.end_date

    main(start_date, end_date)
