import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger

# Sheffield coordinates
LAT = 53.3811
LON = -1.4701

API_URL = (
    f"https://api.open-meteo.com/v1/forecast?"
    f"latitude={LAT}&longitude={LON}"
    "&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
)

logger.add("logs/weather_pipeline.log", rotation="10 MB")


def fetch_weather():

    logger.info("Requesting weather data")

    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()

    data = response.json()

    return data["current"]


def save_to_parquet(record):

    df = pd.DataFrame([record])

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    output_dir = Path(f"data/raw/weather/date={date_str}")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{datetime.now(timezone.utc).strftime('%H-%M-%S')}.parquet"

    file_path = output_dir / file_name

    df.to_parquet(file_path, index=False)

    logger.info(f"Saved weather data to {file_path}")


def main():

    logger.info("Starting weather ingestion")

    data = fetch_weather()

    record = {
        "timestamp": datetime.now(timezone.utc),
        "temperature": data.get("temperature_2m"),
        "humidity": data.get("relative_humidity_2m"),
        "wind_speed": data.get("wind_speed_10m")
    }

    save_to_parquet(record)

    logger.info("Weather pipeline completed")


if __name__ == "__main__":
    main()