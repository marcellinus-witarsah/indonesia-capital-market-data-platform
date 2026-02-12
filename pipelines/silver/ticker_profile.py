import json
from datetime import datetime

import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               LongType, StringType, StructField, StructType,
                               TimestampType)

from src.utils.iceberg_table_operations import IcebergTableOperations
from src.utils.logger import logger


def main(start_date, end_date):
    try:
        # -----------------------------------------------------------
        # Create Spark Session
        # -----------------------------------------------------------
        spark = SparkSession.builder.appName("TickerInfoToJson").getOrCreate()
        logger.info("Spark Session created successfully.")

        # -----------------------------------------------------------
        # Define schema for parsing JSON string from `info` column
        # -----------------------------------------------------------
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
                StructField("previousClose", DoubleType(), True),
                StructField("open", DoubleType(), True),
                StructField("dayLow", DoubleType(), True),
                StructField("dayHigh", DoubleType(), True),
                StructField("regularMarketPreviousClose", DoubleType(), True),
                StructField("regularMarketOpen", DoubleType(), True),
                StructField("regularMarketDayLow", DoubleType(), True),
                StructField("regularMarketDayHigh", DoubleType(), True),
                StructField("bid", DoubleType(), True),
                StructField("ask", DoubleType(), True),
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
                StructField("priceToSalesTrailing12Months", DoubleType(), True),
                StructField("priceToBook", DoubleType(), True),
                # --- Dividends ---
                StructField("dividendRate", DoubleType(), True),
                StructField("dividendYield", DoubleType(), True),
                StructField("exDividendDate", LongType(), True),
                StructField("payoutRatio", DoubleType(), True),
                StructField("fiveYearAvgDividendYield", DoubleType(), True),
                StructField("trailingAnnualDividendRate", DoubleType(), True),
                StructField("trailingAnnualDividendYield", DoubleType(), True),
                StructField("lastDividendValue", DoubleType(), True),
                StructField("lastDividendDate", LongType(), True),
                # --- Risk & Performance ---
                StructField("beta", DoubleType(), True),
                StructField("trailingPE", DoubleType(), True),
                StructField("forwardPE", DoubleType(), True),
                StructField("trailingPegRatio", DoubleType(), True),
                # --- Shares ---
                StructField("floatShares", LongType(), True),
                StructField("sharesOutstanding", LongType(), True),
                StructField("impliedSharesOutstanding", LongType(), True),
                StructField("heldPercentInsiders", DoubleType(), True),
                StructField("heldPercentInstitutions", DoubleType(), True),
                # --- Financials ---
                StructField("bookValue", DoubleType(), True),
                StructField("totalCash", LongType(), True),
                StructField("totalCashPerShare", DoubleType(), True),
                StructField("totalDebt", LongType(), True),
                StructField("debtToEquity", DoubleType(), True),
                StructField("quickRatio", DoubleType(), True),
                StructField("currentRatio", DoubleType(), True),
                StructField("totalRevenue", LongType(), True),
                StructField("revenuePerShare", DoubleType(), True),
                StructField("grossProfits", LongType(), True),
                StructField("ebitda", LongType(), True),
                StructField("freeCashflow", LongType(), True),
                StructField("operatingCashflow", LongType(), True),
                # --- Margins ---
                StructField("profitMargins", DoubleType(), True),
                StructField("grossMargins", DoubleType(), True),
                StructField("ebitdaMargins", DoubleType(), True),
                StructField("operatingMargins", DoubleType(), True),
                # --- Growth ---
                StructField("earningsGrowth", DoubleType(), True),
                StructField("earningsQuarterlyGrowth", DoubleType(), True),
                StructField("revenueGrowth", DoubleType(), True),
                # --- EPS ---
                StructField("trailingEps", DoubleType(), True),
                StructField("forwardEps", DoubleType(), True),
                StructField("epsTrailingTwelveMonths", DoubleType(), True),
                StructField("epsForward", DoubleType(), True),
                StructField("epsCurrentYear", DoubleType(), True),
                StructField("priceEpsCurrentYear", DoubleType(), True),
                # --- Ranges ---
                StructField("fiftyTwoWeekLow", DoubleType(), True),
                StructField("fiftyTwoWeekHigh", DoubleType(), True),
                StructField("allTimeHigh", DoubleType(), True),
                StructField("allTimeLow", DoubleType(), True),
                StructField("fiftyTwoWeekRange", StringType(), True),
                StructField("regularMarketDayRange", StringType(), True),
                # --- Averages ---
                StructField("fiftyDayAverage", DoubleType(), True),
                StructField("twoHundredDayAverage", DoubleType(), True),
                StructField("fiftyDayAverageChange", DoubleType(), True),
                StructField("fiftyDayAverageChangePercent", DoubleType(), True),
                StructField("twoHundredDayAverageChange", DoubleType(), True),
                StructField("twoHundredDayAverageChangePercent", DoubleType(), True),
                # --- Analyst ---
                StructField("targetHighPrice", DoubleType(), True),
                StructField("targetLowPrice", DoubleType(), True),
                StructField("targetMeanPrice", DoubleType(), True),
                StructField("targetMedianPrice", DoubleType(), True),
                StructField("recommendationMean", DoubleType(), True),
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

        # -----------------------------------------------------------
        # Load Spark Table
        # -----------------------------------------------------------
        df = spark.table(f"indonesia_capital_market_catalog.bronze.ticker_info")
        logger.info("Spark table loaded successfully.")

        # -----------------------------------------------------------
        # Instantiate Iceberg Table Operations
        # -----------------------------------------------------------
        iceberg_table_ops = IcebergTableOperations(spark)

        # -----------------------------------------------------------
        # Load Spark Table
        # -----------------------------------------------------------
        df = iceberg_table_ops.get_latest_record(
            "indonesia_capital_market_catalog.bronze.ticker_info",
            ["ticker"],
            ["load_dttm"],
            True,
            start_date,
            end_date,
        )
        logger.info("Spark table loaded successfully.")

        # -----------------------------------------------------------
        # Transformation logic
        # -----------------------------------------------------------
        df = (
            df.alias("ticker_info")
            .withColumn("parsed_info", from_json(col("ticker_info.info"), schema))
            .select(
                col("ticker_info.ticker"),
                col("parsed_info.longName").alias("company_name"),
                col("parsed_info.longBusinessSummary").alias("business_summary"),
                col("parsed_info.currency").alias("currency"),
                col("parsed_info.marketCap").alias("market_cap"),
                col("parsed_info.industry").alias("industry"),
            )
        )
        df = df.withColumn("load_dttm", lit(datetime.now()).cast(TimestampType()))
        logger.info("Data transformation completed.")

        # -----------------------------------------------------------
        # Perform Upsert
        # -----------------------------------------------------------
        df.createOrReplaceTempView("view_ticker_profile")
        query = f"""
        MERGE INTO indonesia_capital_market_catalog.silver.{__file__.split('/')[-1].split('.')[0]} AS target
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
    # load_dotenv(find_dotenv())

    # Prepare arguments
    start_date = "2026-01-01"
    end_date = "2026-12-31"
    main(start_date, end_date)
