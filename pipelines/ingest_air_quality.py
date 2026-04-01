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

output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

file_path = output_dir / "air_quality.csv"

if file_path.exists():
    df.to_csv(file_path, mode="a", header=False, index=False)
else:
    df.to_csv(file_path, index=False)

print("Air quality data saved:", record)
