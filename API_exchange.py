import os
import pandas as pd
import requests
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(override=True)

URL = os.getenv("URL")
API_KEY = os.getenv("API_KEY")

headers = {
    "Authorization": API_KEY
}

target_date = date.today() - timedelta(days=5)

params = {
    "start_period": target_date.strftime("%Y-%m-%d"),
    "end_period": target_date.strftime("%Y-%m-%d")
}

response = requests.get(URL, headers=headers, params=params)
data = response.json()

data_list = (
    data
    .get("result", {})
    .get("data", {})
    .get("data_detail", [])
)

result = [
    {
        "currency": item.get("currency_id"),
        "mid_rate": float(item.get("mid_rate"))
    }
    for item in data_list
    if item.get("mid_rate")
]

df = pd.DataFrame(result)
df.to_csv("currency_THrate.csv", index=False)
