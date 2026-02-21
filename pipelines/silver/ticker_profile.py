import argparse
import os
from datetime import datetime

from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utils.common_functions import string_cleanser
from src.utils.iceberg_table_operations import IcebergTableOperations
from src.utils.logger import logger


def main(start_date, end_date):
    try:
        # Create Spark Session
        spark = SparkSession.builder.appName("TickerInfoToJson").getOrCreate()
        logger.info("Spark Session created successfully.")

        # Define schema for parsing JSON string from `info` column
        schema = StructType(
            [
                # --- Company Info ---
                StructField("address1", StringType(), True),
                StructField("city", StringType(), True),
                StructField("zip", StringType(), True),
                StructField("country", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("fax", StringType(), True),
                StructField("website", StringType(), True),
                StructField("industry", StringType(), True),
                StructField("industryKey", StringType(), True),
                StructField("industryDisp", StringType(), True),
                StructField("sector", StringType(), True),
                StructField("sectorKey", StringType(), True),
                StructField("sectorDisp", StringType(), True),
                StructField("longBusinessSummary", StringType(), True),
                StructField("fullTimeEmployees", LongType(), True),
                # --- Company Officers ---
                StructField(
                    "companyOfficers",
                    ArrayType(
                        StructType(
                            [
                                StructField("maxAge", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("age", IntegerType(), True),
                                StructField("title", StringType(), True),
                                StructField("yearBorn", IntegerType(), True),
                                StructField("exercisedValue", LongType(), True),
                                StructField("unexercisedValue", LongType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                StructField("irWebsite", StringType(), True),
                StructField("executiveTeam", ArrayType(StructType([])), True),
                # --- Market Meta ---
                StructField("maxAge", LongType(), True),
                StructField("priceHint", IntegerType(), True),
                StructField("currency", StringType(), True),
                StructField("financialCurrency", StringType(), True),
                StructField("symbol", StringType(), True),
                # --- Prices ---
                StructField("previousClose", FloatType(), True),
                StructField("open", FloatType(), True),
                StructField("dayLow", FloatType(), True),
                StructField("dayHigh", FloatType(), True),
                StructField("regularMarketPreviousClose", FloatType(), True),
                StructField("regularMarketOpen", FloatType(), True),
                StructField("regularMarketDayLow", FloatType(), True),
                StructField("regularMarketDayHigh", FloatType(), True),
                StructField("bid", FloatType(), True),
                StructField("ask", FloatType(), True),
                StructField("bidSize", LongType(), True),
                StructField("askSize", LongType(), True),
                # --- Volume ---
                StructField("volume", LongType(), True),
                StructField("regularMarketVolume", LongType(), True),
                StructField("averageVolume", LongType(), True),
                StructField("averageVolume10days", LongType(), True),
                StructField("averageDailyVolume10Day", LongType(), True),
                StructField("averageDailyVolume3Month", LongType(), True),
                # --- Market Cap & Valuation ---
                StructField("marketCap", LongType(), True),
                StructField("enterpriseValue", LongType(), True),
                StructField("priceToSalesTrailing12Months", FloatType(), True),
                StructField("priceToBook", FloatType(), True),
                # --- Dividends ---
                StructField("dividendRate", FloatType(), True),
                StructField("dividendYield", FloatType(), True),
                StructField("exDividendDate", LongType(), True),
                StructField("payoutRatio", FloatType(), True),
                StructField("fiveYearAvgDividendYield", FloatType(), True),
                StructField("trailingAnnualDividendRate", FloatType(), True),
                StructField("trailingAnnualDividendYield", FloatType(), True),
                StructField("lastDividendValue", FloatType(), True),
                StructField("lastDividendDate", LongType(), True),
                # --- Risk & Performance ---
                StructField("beta", FloatType(), True),
                StructField("trailingPE", FloatType(), True),
                StructField("forwardPE", FloatType(), True),
                StructField("trailingPegRatio", FloatType(), True),
                # --- Shares ---
                StructField("floatShares", LongType(), True),
                StructField("sharesOutstanding", LongType(), True),
                StructField("impliedSharesOutstanding", LongType(), True),
                StructField("heldPercentInsiders", FloatType(), True),
                StructField("heldPercentInstitutions", FloatType(), True),
                # --- Financials ---
                StructField("bookValue", FloatType(), True),
                StructField("totalCash", LongType(), True),
                StructField("totalCashPerShare", FloatType(), True),
                StructField("totalDebt", LongType(), True),
                StructField("debtToEquity", FloatType(), True),
                StructField("quickRatio", FloatType(), True),
                StructField("currentRatio", FloatType(), True),
                StructField("totalRevenue", LongType(), True),
                StructField("revenuePerShare", FloatType(), True),
                StructField("grossProfits", LongType(), True),
                StructField("ebitda", LongType(), True),
                StructField("freeCashflow", LongType(), True),
                StructField("operatingCashflow", LongType(), True),
                # --- Margins ---
                StructField("profitMargins", FloatType(), True),
                StructField("grossMargins", FloatType(), True),
                StructField("ebitdaMargins", FloatType(), True),
                StructField("operatingMargins", FloatType(), True),
                # --- Growth ---
                StructField("earningsGrowth", FloatType(), True),
                StructField("earningsQuarterlyGrowth", FloatType(), True),
                StructField("revenueGrowth", FloatType(), True),
                # --- EPS ---
                StructField("trailingEps", FloatType(), True),
                StructField("forwardEps", FloatType(), True),
                StructField("epsTrailingTwelveMonths", FloatType(), True),
                StructField("epsForward", FloatType(), True),
                StructField("epsCurrentYear", FloatType(), True),
                StructField("priceEpsCurrentYear", FloatType(), True),
                # --- Ranges ---
                StructField("fiftyTwoWeekLow", FloatType(), True),
                StructField("fiftyTwoWeekHigh", FloatType(), True),
                StructField("allTimeHigh", FloatType(), True),
                StructField("allTimeLow", FloatType(), True),
                StructField("fiftyTwoWeekRange", StringType(), True),
                StructField("regularMarketDayRange", StringType(), True),
                # --- Averages ---
                StructField("fiftyDayAverage", FloatType(), True),
                StructField("twoHundredDayAverage", FloatType(), True),
                StructField("fiftyDayAverageChange", FloatType(), True),
                StructField("fiftyDayAverageChangePercent", FloatType(), True),
                StructField("twoHundredDayAverageChange", FloatType(), True),
                StructField("twoHundredDayAverageChangePercent", FloatType(), True),
                # --- Analyst ---
                StructField("targetHighPrice", FloatType(), True),
                StructField("targetLowPrice", FloatType(), True),
                StructField("targetMeanPrice", FloatType(), True),
                StructField("targetMedianPrice", FloatType(), True),
                StructField("recommendationMean", FloatType(), True),
                StructField("recommendationKey", StringType(), True),
                StructField("numberOfAnalystOpinions", IntegerType(), True),
                StructField("averageAnalystRating", StringType(), True),
                # --- Exchange ---
                StructField("exchange", StringType(), True),
                StructField("exchangeTimezoneName", StringType(), True),
                StructField("exchangeTimezoneShortName", StringType(), True),
                StructField("gmtOffSetMilliseconds", LongType(), True),
                StructField("market", StringType(), True),
                StructField("fullExchangeName", StringType(), True),
                # --- Flags ---
                StructField("tradeable", BooleanType(), True),
                StructField("triggerable", BooleanType(), True),
                StructField("cryptoTradeable", BooleanType(), True),
                StructField("hasPrePostMarketData", BooleanType(), True),
                StructField("esgPopulated", BooleanType(), True),
                StructField("isEarningsDateEstimate", BooleanType(), True),
                # --- Identifiers ---
                StructField("quoteType", StringType(), True),
                StructField("typeDisp", StringType(), True),
                StructField("quoteSourceName", StringType(), True),
                StructField("messageBoardId", StringType(), True),
                # --- Timestamps (epoch, no timezone) ---
                StructField("lastFiscalYearEnd", LongType(), True),
                StructField("nextFiscalYearEnd", LongType(), True),
                StructField("mostRecentQuarter", LongType(), True),
                StructField("earningsTimestamp", LongType(), True),
                StructField("earningsTimestampStart", LongType(), True),
                StructField("earningsTimestampEnd", LongType(), True),
                StructField("earningsCallTimestampStart", LongType(), True),
                StructField("earningsCallTimestampEnd", LongType(), True),
                StructField("firstTradeDateMilliseconds", LongType(), True),
                StructField("regularMarketTime", LongType(), True),
                # --- Misc ---
                StructField("language", StringType(), True),
                StructField("region", StringType(), True),
                StructField("customPriceAlertConfidence", StringType(), True),
                StructField("marketState", StringType(), True),
                StructField("sourceInterval", IntegerType(), True),
                StructField("exchangeDataDelayedBy", IntegerType(), True),
                StructField("shortName", StringType(), True),
                StructField("longName", StringType(), True),
                StructField("corporateActions", ArrayType(StructType([])), True),
            ]
        )

        # Instantiate Iceberg Table Operations
        iceberg_table_ops = IcebergTableOperations(spark)

        # -----------------------------------------------------------
        # Data Load
        # -----------------------------------------------------------
        df = iceberg_table_ops.get_latest_record(
            f"{os.getenv('CATALOG_NAME')}.bronze.ticker_info",
            ["ticker"],
            ["load_dttm"],
            True,
            start_date,
            end_date,
        )
        logger.info("Spark table loaded successfully.")

        # -----------------------------------------------------------
        # Data Transformation
        # -----------------------------------------------------------
        df = (
            df.alias("ticker_info")
            .withColumn("parsed_info", from_json(col("ticker_info.info"), schema))
            .select(
                string_cleanser(col("ticker_info.ticker"))
                .astype(StringType())
                .alias("ticker"),
                string_cleanser(col("parsed_info.longName"))
                .astype(StringType())
                .alias("company_name"),
                string_cleanser(col("parsed_info.longBusinessSummary"))
                .astype(StringType())
                .alias("business_summary"),
                string_cleanser(col("parsed_info.currency"))
                .astype(StringType())
                .alias("currency"),
                col("parsed_info.marketCap").astype(FloatType()).alias("market_cap"),
                string_cleanser(col("parsed_info.industry"))
                .astype(StringType())
                .alias("industry"),
            )
            .distinct()
        )
        df = df.withColumn("load_dttm", lit(datetime.now()).cast(TimestampType()))
        logger.info("Data transformation completed.")

        # -----------------------------------------------------------
        # Data Ingestion
        # -----------------------------------------------------------
        df.createOrReplaceTempView("view_ticker_profile")
        query = f"""
        MERGE INTO {os.getenv('CATALOG_NAME')}.silver.{__file__.split('/')[-1].split('.')[0]} AS target
        USING view_ticker_profile AS source
        ON target.ticker = source.ticker
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
