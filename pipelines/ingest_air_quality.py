import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger
import time

API_TOKEN = "50aed127932c7def831bf1329b5eb4204f6a7e02"

API_URL = f"https://api.waqi.info/feed/sheffield/?token={API_TOKEN}"

#Configure logging
logger.add("logs/pipeline.log", rotation="10 MB")

def fetch_air_quality():

    for attempt in range(3):

        try:
            logger.info("Requesting air quality data")

            response = requests.get(API_URL, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data["status"] != "ok":
                raise Exception("API returned non-ok status")

            return data["data"]

        except Exception as e:

            logger.warning(f"Attempt {attempt+1} failed: {e}")

            time.sleep(2)

    raise Exception("API request failed after 3 retries")


def save_to_parquet(record):

    df = pd.DataFrame([record])

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    output_dir = Path(f"data/raw/air_quality/date={date_str}")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{datetime.now(timezone.utc).strftime('%H-%M-%S')}.parquet"

    file_path = output_dir / file_name

    df.to_parquet(file_path, index=False)

    logger.info(f"Saved data to {file_path}")


def main():

    logger.info("Starting air quality ingestion")

    data = fetch_air_quality()

    record = {
        "timestamp": datetime.now(timezone.utc),
        "aqi": data.get("aqi"),
        "pm25": data["iaqi"].get("pm25", {}).get("v"),
        "pm10": data["iaqi"].get("pm10", {}).get("v"),
        "temperature": data["iaqi"].get("t", {}).get("v"),
        "humidity": data["iaqi"].get("h", {}).get("v")
    }

    save_to_parquet(record)

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()