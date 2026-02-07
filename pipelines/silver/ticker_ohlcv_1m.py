import json
from datetime import datetime
import yfinance as yf
from pyspark.sql.functions import lit, col, to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession



def main():
    # -----------------------------------------------------------
    # Create Spark Session
    # -----------------------------------------------------------
    spark = SparkSession.builder.appName("silver_pipeline").getOrCreate()

    # -----------------------------------------------------------
    # Load Spark Table
    # -----------------------------------------------------------
    df = spark.table(f"indonesia_capital_market_catalog.bronze.market_data_1m")

    # -----------------------------------------------------------
    # Transformation logic
    # -----------------------------------------------------------
    df = (
        df.alias("market_data")
        .select(
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
    )

    df = df.withColumn("load_dttm",  lit(datetime.now()).cast(TimestampType()))

    df.createOrReplaceTempView("view_ticker_ohlcv_1m")

    # -----------------------------------------------------------
    # Perform Upsert
    # -----------------------------------------------------------
    query = f"""
    MERGE INTO indonesia_capital_market_catalog.silver.{__file__.split('/')[-1].split('.')[0]} AS target
    USING view_ticker_ohlcv_1m AS source
    ON target.ticker = source.ticker
    AND target.datetime = source.datetime
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;
    """

    # Upsert to Iceberg Table
    spark.sql(query)

if __name__ == "__main__":
    # Load environment variables
    # load_dotenv(find_dotenv())
    
    # Prepare arguments
    # bucket_name = "iceberg"
    # content_type = "application/json"
    main()