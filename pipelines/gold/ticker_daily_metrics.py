from datetime import datetime
from pyspark.sql.functions import lit, col, avg, to_date, sqrt, sum, avg, count, pow, row_number
from pyspark.sql import Window
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession


def get_partitioned_row_number(df, partition_cols, order_cols, order=None):
    try:
        if order == "asc":
            df = (df.withColumn("row_number",row_number().over(
                    Window.partitionBy(
                        *[col(key) for key in partition_cols]
                    )
                    .orderBy(*[col(key).asc() for key in order_cols])
                ))
            )
        elif order == "desc":
            df = (df.withColumn("row_number",row_number().over(
                    Window.partitionBy(
                        *[col(key) for key in partition_cols]
                    )
                    .orderBy(*[col(key).desc() for key in order_cols])
                ))
            )
    except Exception as e:
        print(f"Error in get_partitioned_row_number: {e}")
        raise e
    return df


def main():
    # -----------------------------------------------------------
    # Create Spark Session
    # -----------------------------------------------------------
    spark = SparkSession.builder.appName("silver_pipeline").getOrCreate()

    # -----------------------------------------------------------
    # Load Spark Table
    # -----------------------------------------------------------
    df = spark.table(f"indonesia_capital_market_catalog.silver.ticker_ohlcv_1m")
    df = df.withColumn("date", to_date(col("datetime"))) 

    # -----------------------------------------------------------
    # Transformation logic
    # -----------------------------------------------------------
    # 1. Daily Returns: percentage change between the closing price and opening price of the day
    df_daily_first_record = get_partitioned_row_number(df, ["ticker", "date"], ["datetime"], order="asc").filter(col("row_number") == 1)
    df_daily_last_record = get_partitioned_row_number(df, ["ticker", "date"], ["datetime"], order="desc").filter(col("row_number") == 1)
    df_daily_returns = (
        df_daily_last_record.alias("daily_last_record")
        .join(
            df_daily_first_record.alias("daily_first_record"),
            (col("daily_last_record.ticker") == col("daily_first_record.ticker")) & (col("daily_last_record.date") == col("daily_first_record.date")),
            "inner"
        )
        .select(
            col("daily_last_record.ticker").alias("ticker"),
            col("daily_last_record.date").alias("date"),
            ((col("daily_last_record.close") - col("daily_first_record.open")) / col("daily_first_record.open")).alias("daily_returns")
        )
    )

    # 2. Daily Volatility: standard deviation of the closing prices throughout the day
    df_daily_average_price_by_ticker_data = (
        df.alias("ticker_ohlcv_1m")
        .groupBy(col("ticker_ohlcv_1m.ticker"), col("date"))
        .agg(
            avg(col("ticker_ohlcv_1m.close")).alias("avg_close"),
        )
    )
    df_sse = (
        df.alias("ticker_ohlcv_1m")
        .join(
            df_daily_average_price_by_ticker_data.alias("df_daily_average_price_by_ticker_data"),
            (col("ticker_ohlcv_1m.ticker") == col("df_daily_average_price_by_ticker_data.ticker"))
            & (col("ticker_ohlcv_1m.date") == col("df_daily_average_price_by_ticker_data.date")),
            "left"
        )
        .withColumn("squared_differences", pow(col("ticker_ohlcv_1m.close") - col("df_daily_average_price_by_ticker_data.avg_close"), 2))
        .groupBy(
            col("ticker_ohlcv_1m.ticker"),
            col("ticker_ohlcv_1m.date")
        )
        .agg(
            sum(col("squared_differences")).alias("sum_squared_errors"),
            count(col("ticker_ohlcv_1m.ticker")).alias("n")
        )
    )
    df_daily_volatility = (
        df_sse
        .select(
            col("ticker"),
            col("date"),
            sqrt(col("sum_squared_errors") / col("n")).alias("daily_volatility"),
        )
    )

    # -----------------------------------------------------------
    # Create Gold DataFrame
    # -----------------------------------------------------------
    gold_df = (
        df_daily_returns.alias("daily_returns")
        .join(
            df_daily_volatility.alias("daily_volatility"),
            (col("daily_returns.ticker") == col("daily_volatility.ticker")) 
            & (col("daily_returns.date") == col("daily_volatility.date")),
            "left"
        ).select(        
            col("daily_returns.ticker").alias("ticker"),
            col("daily_returns.date").alias("date"),
            col("daily_returns.daily_returns").alias("daily_returns"),
            col("daily_volatility.daily_volatility").alias("daily_volatility")
        )
    )
    gold_df = gold_df.withColumn("load_dttm",  lit(datetime.now()).cast(TimestampType()))
    gold_df.createOrReplaceTempView("view_ticker_daily_metrics")

    # -----------------------------------------------------------
    # Perform Upsert
    # -----------------------------------------------------------
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