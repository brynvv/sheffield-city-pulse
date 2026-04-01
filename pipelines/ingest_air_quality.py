import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

API_TOKEN = "50aed127932c7def831bf1329b5eb4204f6a7e02"

url = f"https://api.waqi.info/feed/sheffield/?token={API_TOKEN}"

response = requests.get(url)
data = response.json()

if data["status"] != "ok":
    raise Exception("API request failed")

aqi_data = data["data"]

record = {
    "timestamp": datetime.utcnow(),
    "aqi": aqi_data.get("aqi"),
    "pm25": aqi_data["iaqi"].get("pm25", {}).get("v"),
    "pm10": aqi_data["iaqi"].get("pm10", {}).get("v"),
    "temperature": aqi_data["iaqi"].get("t", {}).get("v"),
    "humidity": aqi_data["iaqi"].get("h", {}).get("v")
}

df = pd.DataFrame([record])

# Create partition based on date
date_str = datetime.utcnow().strftime("%Y-%m-%d")

output_dir = Path(f"data/raw/air_quality/date={date_str}")
output_dir.mkdir(parents=True, exist_ok=True)

file_name = f"{datetime.utcnow().strftime('%H-%M-%S')}.parquet"

file_path = output_dir / file_name

df.to_parquet(file_path, index=False)

print("Saved air quality data to:", file_path)