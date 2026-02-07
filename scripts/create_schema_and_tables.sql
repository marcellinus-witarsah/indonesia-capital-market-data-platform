USE indonesia_capital_market_catalog;

-- Create schema for each layer
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- CREATE TABLE IF NOT EXISTS for each silver and gold layer
-- 1. Silver Layer Tables

CREATE TABLE IF NOT EXISTS silver.ticker_profile (
    ticker STRING,
    load_dttm TIMESTAMP
)
USING iceberg;

CREATE TABLE IF NOT EXISTS silver.ticker_ohlcv_1m (
    ticker STRING,
    datetime TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    dividends FLOAT,
    stock_splits FLOAT,
    load_dttm TIMESTAMP
)
USING iceberg;

-- 2. Gold Layer Tables
CREATE TABLE IF NOT EXISTS gold.ticker_daily_metrics (
    ticker STRING,
    date DATE,
    daily_returns FLOAT,
    daily_volatility FLOAT,
    load_dttm TIMESTAMP
)
USING iceberg;