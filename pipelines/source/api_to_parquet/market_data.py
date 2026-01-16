import json
import os
import shutil
from datetime import datetime
import yfinance as yf
from dotenv import find_dotenv, load_dotenv
from src.utils.minio import MinioClient

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


def main(period="2d", interval="1m", bucket_name=None, content_type=None):
    # Connect to Minio
    client = MinioClient(
        os.getenv("MINIO_HOST"),
        os.getenv("MINIO_ROOT_USER"),
        os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )

    for ticker in TICKERS:
        # Create a temporary folder to store the 
        os.makedirs(f"./tmp/{datetime.now().strftime('%Y-%m-%d')}/{__file__.split('/')[-1]}", exist_ok=True)

        # Save company market data to a parquet file
        df = yf.Ticker(ticker).history(period=period, interval=interval).reset_index()
        df.to_parquet(
            f"./tmp/{datetime.now().strftime('%Y-%m-%d')}/{__file__.split('/')[-1]}/{ticker}.parquet"
        )

        # Upload file to the Minio bucket
        object_name = f"source/{datetime.now().strftime('%Y-%m-%d')}/{__file__.split('/')[-1]}/{ticker}.parquet"
        file_path = (
            f"./tmp/{datetime.now().strftime('%Y-%m-%d')}/{__file__.split('/')[-1]}/{ticker}.parquet"
        )
        client.upload_file(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            content_type=content_type,
        )

        # Delete a temporary folder and all of its contents
        shutil.rmtree(f"./tmp/{datetime.now().strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    # Load environment variables
    load_dotenv(find_dotenv())
    
    # Prepare arguments
    bucket_name = "iceberg"
    content_type = "application/json"
    main(bucket_name=bucket_name, content_type=content_type)