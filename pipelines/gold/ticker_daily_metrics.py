from datetime import datetime
from pyspark.sql.functions import lit, col, avg, to_date, sqrt, sum, min, max, avg, count, pow
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession




def main():
    # Create Spark session
    spark = SparkSession.builder.appName("silver_pipeline").getOrCreate()

    # Define Spark Table Schema
    df = spark.table(f"indonesia_capital_market_catalog.silver.ticker_ohlcv_1m")
    df = df.withColumn("date", to_date(col("datetime"))) 

    df_agg = (
        df.alias("ticker_ohlcv_1m")
        .groupBy(col("ticker_ohlcv_1m.ticker"), col("date"))
        .agg(
            max(col("ticker_ohlcv_1m.close")).alias("max_close"),
            min(col("ticker_ohlcv_1m.open")).alias("min_open"),
            avg(col("ticker_ohlcv_1m.close")).alias("avg_close"),
        )
    )

    # For computing daily returns
    df_metric_daily_returns = (
        df_agg.alias("ticker_ohlcv_1d")
        .select(
            col("ticker_ohlcv_1d.ticker"),
            col("ticker_ohlcv_1d.date"),
            ((col("ticker_ohlcv_1d.max_close") - col("ticker_ohlcv_1d.min_open"))/col("ticker_ohlcv_1d.min_open")).alias("daily_returns"),
        )
    )

    # For computing volatility
    df_sse = (
        df.alias("ticker_ohlcv_1m")
        .join(
            df_agg.alias("ticker_ohlcv_1d"),
            (col("ticker_ohlcv_1m.ticker") == col("ticker_ohlcv_1d.ticker"))
            & (col("ticker_ohlcv_1m.date") == col("ticker_ohlcv_1d.date")),
            "left"
        )
        .withColumn("squared_differences", pow(col("ticker_ohlcv_1m.close") - col("ticker_ohlcv_1d.avg_close"), 2))
        .groupBy(
            col("ticker_ohlcv_1m.ticker"),
            col("ticker_ohlcv_1m.date")
        )
        .agg(
            sum(col("squared_differences")).alias("sum_squared_errors"),
            count(col("ticker_ohlcv_1m.datetime")).alias("n")
        )
    )

    df_metric_daily_volatility = (
        df_sse
        .select(
            col("ticker"),
            col("date"),
            sqrt(col("sum_squared_errors") / col("n")).alias("daily_volatility"),
        )
    )    

    gold_df = (
        df_metric_daily_returns.alias("metric_daily_returns")
        .join(
            df_metric_daily_volatility.alias("metric_daily_volatility"),
            (col("metric_daily_returns.ticker") == col("metric_daily_volatility.ticker")) 
            & (col("metric_daily_returns.date") == col("metric_daily_volatility.date")),
            "left"
        ).select(        
            col("metric_daily_returns.ticker").alias("ticker"),
            col("metric_daily_returns.date").alias("date"),
            col("metric_daily_returns.daily_returns").alias("daily_returns"),
            col("metric_daily_volatility.daily_volatility").alias("daily_volatility")
        )
    )

    gold_df = gold_df.withColumn("load_dttm",  lit(datetime.now()).cast(TimestampType()))

    gold_df.createOrReplaceTempView("view_ticker_daily_metrics")

    # Create Query for Upsert
    query = f"""
        MERGE INTO indonesia_capital_market_catalog.gold.{__file__.split('/')[-1].split('.')[0]} AS target
        USING view_ticker_daily_metrics AS source
        ON target.ticker = source.ticker
        AND target.date = source.date
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