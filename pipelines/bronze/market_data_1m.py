import argparse
import os
from datetime import datetime

import pandas as pd
import yfinance as yf
from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType, TimestampType

from src.utils.logger import logger


def main(tickers, period, interval="1m"):
    try:
        # -----------------------------------------------------------
        # Create Spark Session
        # -----------------------------------------------------------
        spark = SparkSession.builder.appName("MarketData").getOrCreate()
        logger.info("Spark Session created successfully.")

        # -----------------------------------------------------------
        # Get data form source
        # -----------------------------------------------------------
        data = pd.DataFrame()
        for ticker in tickers:
            stock_data = (
                yf.Ticker(ticker)
                .history(period=period, interval=interval)
                .reset_index()
            )
            stock_data["Ticker"] = ticker
            data = pd.concat([data, stock_data], ignore_index=True)
        logger.info("Data retrieved from API successfully.")

        # -----------------------------------------------------------
        # Create Spark Dataframe
        # -----------------------------------------------------------
        df = spark.createDataFrame(data)
        df = df.withColumn("load_dttm", lit(datetime.now()).cast(TimestampType()))
        df = df.withColumn("load_prdt", lit(datetime.now().date()).cast(DateType()))
        logger.info("Spark Table Create Successfully.")

        # -----------------------------------------------------------
        # Write to Iceberg Table
        # -----------------------------------------------------------
        df.write.format("iceberg").partitionBy("load_prdt").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(
            f"{os.getenv('CATALOG_NAME')}.bronze.{__file__.split('/')[-1].split('.')[0]}"
        )
        logger.info(
            f"Data written to {os.getenv('CATALOG_NAME')}.bronze.{__file__.split('/')[-1].split('.')[0]} successfully."
        )
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
    parser.add_argument(
        "--period", type=str, help="The period for market data retrieval"
    )
    args = parser.parse_args()

    # Insert arguments
    tickers = args.ticker
    period = args.period

    logger.info(f"Tickers: {tickers}, Period: {period}")

    # Run the main function
    main(tickers, period)
