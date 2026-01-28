import json
from datetime import datetime
import yfinance as yf
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DateType
from pyspark.sql import SparkSession

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
    # Create Spark session
    spark = SparkSession.builder.appName("TickerInfoToJson").getOrCreate()


    # Define Spark Table Schema
    schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("info", StringType(), True),
    ])

    data = []
    for ticker in TICKERS:
        data.append(
            (   
                ticker,
                json.dumps(yf.Ticker(ticker).info),
            )
        )
    
    # Create Dataframe
    df = spark.createDataFrame(data, schema=schema)
    df = df.withColumn("load_dttm",  lit(datetime.now()).cast(TimestampType()))
    df = df.withColumn("load_prdt",  lit(datetime.now().date()).cast(DateType()))

    # Write to Iceberg Table
    df.write.format("iceberg") \
        .partitionBy("load_prdt") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"indonesia_capital_market_catalog.bronze.{__file__.split('/')[-1].split('.')[0]}")

if __name__ == "__main__":
    # Load environment variables
    # load_dotenv(find_dotenv())
    
    # Prepare arguments
    bucket_name = "iceberg"
    content_type = "application/json"
    main(bucket_name=bucket_name, content_type=content_type)