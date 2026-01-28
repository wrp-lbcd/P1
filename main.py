import os
import pandas as pd
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
import json

load_dotenv(override=True)

# GET API FROM BANK OF THAILAND CONVERT TO DataFrame
def get_exchange_rate():
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

    response = requests.get(URL, headers=headers, params=params).json()

    data_list = (
        response
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

    df_rate = pd.DataFrame(result)
    return df_rate

df_rate = get_exchange_rate()

# ============================

# READ CSV FILE
df_sales = pd.read_csv("Dataset/ChocolateSales.csv")

# Str > Integer
df_sales["amount"] = (
    df_sales["Amount"]
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
    .astype(float)
)
df_sales.drop(columns=["Amount"], inplace=True)

# Map country = currency
class Config_country:
    country_currency_map = {
        "UK": "GBP",
        "USA": "USD",
        "Australia": "AUD",
        "India": "INR",
        "Canada": "CAD",
        "New Zealand": "AUD"
    }

# Create currency column based on country
df_sales["currency"] = df_sales["Country"].map(Config_country.country_currency_map)

# Type of Date column is Srt and we need to convert it
df_sales["Date"] = pd.to_datetime(df_sales["Date"], format="%d/%m/%Y")

# Change column name (automation)
df_sales.columns = (
    df_sales.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

#Join / Merge
df_final = df_sales.merge(df_rate, how="left", left_on="currency", right_on="currency")

# multiply 2 columns to compute TH rates based on country
df_final["total_amount_th"] = df_final["amount"] * df_final["mid_rate"]

df_final.to_parquet("output.parquet", index=False)
df_final.to_csv("output.csv", index=False)






