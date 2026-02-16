# Databricks notebook source
try:
    CLIENT_ID = dbutils.secrets.get("flight_analytics_secret", "client_id")
    CLIENT_SECRET = dbutils.secrets.get("flight_analytics_secret", "client_secret")
    print("Secrets loaded successfully")
except Exception:
    print("Secrets not found")

# COMMAND ----------

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"

# India bounding box
PARAMS = {
    "lamin": 6,
    "lamax": 37.5,
    "lomin": 68,
    "lomax": 97.5
}

# COMMAND ----------

import requests
token_resp = requests.post(
    TOKEN_URL,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    },
    timeout=30
)

token_resp.raise_for_status()
access_token = token_resp.json()["access_token"]

print("OAuth token received")


# COMMAND ----------

resp = requests.get(
    STATES_URL,
    headers={"Authorization": f"Bearer {access_token}"},
    params=PARAMS,
    timeout=30
)

print("Status code:", resp.status_code)
print("RateLimit-Remaining:", resp.headers.get("X-Rate-Limit-Remaining"))

resp.raise_for_status()

data = resp.json()
print("Flights fetched:", len(data.get("states", [])))


# COMMAND ----------

import os
from datetime import datetime
import json

now = datetime.utcnow()

base_path = (
    f"/Volumes/flight_analytics/bronze/sourcefiles/"
    f"year={now.year}/"
    f"month={now.month:02d}/"
    f"day={now.day:02d}"
)

file_name = f"opensky_{now.strftime('%H%M%S')}.json"
file_path = f"{base_path}/{file_name}"

# Create intermediate directories if they do not exist
os.makedirs(base_path, exist_ok=True)

# Save JSON using standard Python file I/O
with open(file_path, "w") as f:
    f.write(json.dumps(data))

print("Saved Bronze JSON to Unity Catalog volume at:", file_path)