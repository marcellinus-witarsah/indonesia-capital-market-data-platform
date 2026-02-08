import pandas as pd
from datetime import datetime
import yfinance as yf
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType, DateType
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


def main(period="1d"):
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
        for ticker in TICKERS:
            stock_data = yf.Ticker(ticker).history(period=period, interval="1m").reset_index()
            stock_data['Ticker'] = ticker
            data = pd.concat([data, stock_data], ignore_index=True)
        logger.info("Data retrieved from API successfully.")
        
        # -----------------------------------------------------------
        # Create Spark Dataframe
        # -----------------------------------------------------------
        df = spark.createDataFrame(data)
        df = df.withColumn("load_dttm",  lit(datetime.now()).cast(TimestampType()))
        df = df.withColumn("load_prdt",  lit(datetime.now().date()).cast(DateType()))
        logger.info("Spark Table Create Successfully.")

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
    main(period="1d")