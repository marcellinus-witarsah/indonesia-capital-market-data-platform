import json
from datetime import datetime
import yfinance as yf
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DateType
from pyspark.sql import SparkSession
from utils.logger import logger

TICKERS = [
    "BREN.JK",
    "BBCA.JK",
    "DSSA.JK",
    "AMMN.JK",
    "TPIA.JK",
    "BYAN.JK",
    "BBRI.JK",
    "DCII.JK",
    "BMRI.JK",
    "TLKM.JK",
]


def main():
    try:
        # -----------------------------------------------------------
        # Create Spark session
        # -----------------------------------------------------------
        spark = SparkSession.builder.appName("TickerInfoToJson").getOrCreate()
        logger.info("Spark Session created successfully.")


        # -----------------------------------------------------------
        # Define Spark Table Schema
        # -----------------------------------------------------------
        schema = StructType([
            StructField("ticker", StringType(), False),
            StructField("info", StringType(), True),
        ])

        # -----------------------------------------------------------
        # Get data form source
        # -----------------------------------------------------------
        data = []
        for ticker in TICKERS:
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
        df = spark.createDataFrame(data, schema=schema)
        df = df.withColumn("load_dttm",  lit(datetime.now()).cast(TimestampType()))
        df = df.withColumn("load_prdt",  lit(datetime.now().date()).cast(DateType()))
        logger.info("Spark DataFrame created successfully.")

        # -----------------------------------------------------------
        # Write to Iceberg Table
        # -----------------------------------------------------------
        df.write.format("iceberg") \
            .partitionBy("load_prdt") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"indonesia_capital_market_catalog.bronze.{__file__.split('/')[-1].split('.')[0]}")
        logger.info("Data written to Iceberg Table successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e

if __name__ == "__main__":
    # Load environment variables
    # load_dotenv(find_dotenv())
    
    # Prepare arguments
    main()