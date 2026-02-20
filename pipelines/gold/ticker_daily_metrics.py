import argparse
import os
from datetime import datetime

from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, pow, sqrt, sum, to_date
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
        ticker_ohlcv_1m_df = spark.table(
            f"{os.getenv('CATALOG_NAME')}.silver.ticker_ohlcv_1m"
        ).filter(col("datetime").between(start_date, end_date))
        ticker_ohlcv_1m_df = ticker_ohlcv_1m_df.withColumn(
            "date", to_date(col("datetime"))
        )
        logger.info("Spark Table loaded successfully.")

        # -----------------------------------------------------------
        # Data Transformation
        # -----------------------------------------------------------
        # 1. Daily Returns: percentage change between the closing price and opening price of the day
        df_daily_first_record = iceberg_table_ops.get_partitioned_row_number(
            ticker_ohlcv_1m_df, ["ticker", "date"], ["datetime"], order="asc"
        ).filter(col("row_number") == 1)
        df_daily_last_record = iceberg_table_ops.get_partitioned_row_number(
            ticker_ohlcv_1m_df, ["ticker", "date"], ["datetime"], order="desc"
        ).filter(col("row_number") == 1)

        df_daily_returns = (
            df_daily_last_record.alias("daily_last_record")
            .join(
                df_daily_first_record.alias("daily_first_record"),
                (col("daily_last_record.ticker") == col("daily_first_record.ticker"))
                & (col("daily_last_record.date") == col("daily_first_record.date")),
                "inner",
            )
            .select(
                col("daily_last_record.ticker").alias("ticker"),
                col("daily_last_record.date").alias("date"),
                (
                    (col("daily_last_record.close") - col("daily_first_record.open"))
                    / col("daily_first_record.open")
                ).alias("daily_returns"),
            )
        )

        # 2. Daily Volatility: standard deviation of the closing prices throughout the day
        df_daily_average_price_by_ticker_date = (
            ticker_ohlcv_1m_df.alias("ticker_ohlcv_1m")
            .groupBy(col("ticker_ohlcv_1m.ticker"), col("date"))
            .agg(
                avg(col("ticker_ohlcv_1m.close")).alias("avg_close"),
            )
        )
        df_sse = (
            ticker_ohlcv_1m_df.alias("ticker_ohlcv_1m")
            .join(
                df_daily_average_price_by_ticker_date.alias(
                    "df_daily_average_price_by_ticker_date"
                ),
                (
                    col("ticker_ohlcv_1m.ticker")
                    == col("df_daily_average_price_by_ticker_date.ticker")
                )
                & (
                    col("ticker_ohlcv_1m.date")
                    == col("df_daily_average_price_by_ticker_date.date")
                ),
                "left",
            )
            .withColumn(
                "squared_differences",
                pow(
                    col("ticker_ohlcv_1m.close")
                    - col("df_daily_average_price_by_ticker_date.avg_close"),
                    2,
                ),
            )
            .groupBy(col("ticker_ohlcv_1m.ticker"), col("ticker_ohlcv_1m.date"))
            .agg(
                sum(col("squared_differences")).alias("sum_squared_errors"),
                count(col("ticker_ohlcv_1m.ticker")).alias("n"),
            )
        )
        df_daily_volatility = df_sse.select(
            col("ticker"),
            col("date"),
            sqrt(col("sum_squared_errors") / col("n")).alias("daily_volatility"),
        )

        # Create Gold DataFrame
        df = (
            df_daily_returns.alias("daily_returns")
            .join(
                df_daily_volatility.alias("daily_volatility"),
                (col("daily_returns.ticker") == col("daily_volatility.ticker"))
                & (col("daily_returns.date") == col("daily_volatility.date")),
                "left",
            )
            .select(
                col("daily_returns.ticker").alias("ticker"),
                col("daily_returns.date").alias("date"),
                col("daily_returns.daily_returns").alias("daily_returns"),
                col("daily_volatility.daily_volatility").alias("daily_volatility"),
            )
        )
        df = df.withColumn("load_dttm", lit(datetime.now()).cast(TimestampType()))
        logger.info(f"Data transformation completed")

        # -----------------------------------------------------------
        # Data Insertion
        # -----------------------------------------------------------
        df.createOrReplaceTempView("view_ticker_daily_metrics")
        query = f"""
            MERGE INTO {os.getenv('CATALOG_NAME')}.gold.{__file__.split('/')[-1].split('.')[0]} AS target
            USING view_ticker_daily_metrics AS source
            ON target.ticker = source.ticker
            AND target.date = source.date
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
