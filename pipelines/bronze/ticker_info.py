import argparse
import json
import os
from datetime import datetime

import yfinance as yf
from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.logger import logger


def main(tickers):
    try:
        # -----------------------------------------------------------
        # Create Spark session
        # -----------------------------------------------------------
        spark = SparkSession.builder.appName("TickerInfoToJson").getOrCreate()
        logger.info("Spark Session created successfully.")

        # -----------------------------------------------------------
        # Define Spark Table Schema
        # -----------------------------------------------------------
        schema = StructType(
            [
                StructField("ticker", StringType(), False),
                StructField("info", StringType(), True),
            ]
        )

        # # -----------------------------------------------------------
        # # Get data form source
        # # -----------------------------------------------------------
        data = []
        for ticker in tickers:
            data.append(
                (
                    ticker,
                    json.dumps(yf.Ticker(ticker).info),
                )
            )
        logger.info("Data retrieved from API successfully.")

        # -----------------------------------------------------------
        # Create Dataframe
        # -----------------------------------------------------------
        df = spark.createDataFrame(data=data, schema=schema)
        df = df.withColumn("load_dttm", lit(datetime.now()).cast(TimestampType()))
        df = df.withColumn("load_prdt", lit(datetime.now().date()).cast(DateType()))
        logger.info("Spark DataFrame created successfully.")

        # -----------------------------------------------------------
        # Write to Iceberg Table
        # -----------------------------------------------------------
        df.write.format("iceberg").partitionBy("load_prdt").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(
            f"{os.getenv('CATALOG_NAME')}.bronze.{__file__.split('/')[-1].split('.')[0]}"
        )
        logger.info("Data written to Iceberg Table successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e


if __name__ == "__main__":
    # Load environment variables
    load_dotenv(find_dotenv())

    # Prepare arguments
    parser = argparse.ArgumentParser(description="Ticker Info Ingestion Pipeline")
    parser.add_argument(
        "--ticker", nargs="+", help="The ticker symbol(s) to retrieve info for"
    )
    args = parser.parse_args()

    # Insert arguments
    tickers = args.ticker
    main(tickers)
