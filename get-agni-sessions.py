#!/usr/bin/env python3
import requests
import os
import csv
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import sys
import time

# --- 1. Load environment variables ---
load_dotenv()

KEY_ID = os.getenv("KEY_ID")
KEY_VALUE = os.getenv("KEY_VALUE")
ORG_ID = os.getenv("AGNI_ORG_ID")


if not KEY_ID or not KEY_VALUE or not ORG_ID:
    print("Missing KEY_ID, KEY_VALUE, or AGNI_ORG_ID in .env", file=sys.stderr)
    sys.exit(1)

# --- 2. Login and create session ---
session = requests.Session()
login_url = "https://ag01c01.agni.arista.io/cvcue/keyLogin"
login_params = {"keyID": KEY_ID, "keyValue": KEY_VALUE}
login_headers = {"Accept": "application/json"}

try:
    print("Logging in...")
    resp = session.get(login_url, headers=login_headers, params=login_params, timeout=30)
    resp.raise_for_status()
    print("Login successful.")
except requests.exceptions.RequestException as e:
    print(f"Login failed: {e}", file=sys.stderr)
    sys.exit(1)

# --- 3. Prepare API request ---
API_URL = "https://ag01c01.agni.arista.io/api/session.list"
TIMEOUT = 60
WINDOW_MINUTES = 30  # fetch x minutes of data at a time Adjust if you see 1000 records returned. Seems to be the max...
START_DATE = datetime.now(timezone.utc) - timedelta(hours=6)  # How far back to look

all_records = []

# Start from now and move backward in 5-minute windows
current_to = datetime.now(timezone.utc)
current_from = current_to - timedelta(minutes=WINDOW_MINUTES)

print(f"\n Starting fetch using {WINDOW_MINUTES}-minute windows")

while current_from >= START_DATE:
    print(f"\n Fetching sessions from {current_from.isoformat()} → {current_to.isoformat()}")

    payload = {
        "orgID": ORG_ID,
        "status": "failed",
        "fromTimestamp": current_from.isoformat().replace("+00:00", "Z"),
        "toTimestamp": current_to.isoformat().replace("+00:00", "Z"),
    }

    try:
        resp = session.post(API_URL, json=payload, timeout=TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}", file=sys.stderr)
        break

    if "error" in body and body["error"]:
        print(f"API returned error: {body['error']}", file=sys.stderr)
        break

    data = body.get("data", {})
    records = data.get("records", [])

    if not records:
        print("No records returned for this time window.")
    else:
        print(f"  > Retrieved {len(records)} records")
        all_records.extend(records)

    # Move the window back
    current_to = current_from
    current_from = current_to - timedelta(minutes=WINDOW_MINUTES)

    time.sleep(0.2)  # small delay to avoid API rate limits

print(f"\n Total records collected: {len(all_records)}")

# --- 4. Write to CSV ---
if not all_records:
    print("No records to export.")
    sys.exit(0)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_file = f"agni_session_list_{timestamp}.csv"

# Collect all keys from all records for CSV header
all_keys = sorted({k for rec in all_records for k in rec.keys()})

try:
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        writer.writerows(all_records)
    print(f"Wrote {len(all_records)} records to {csv_file}")
except IOError as e:
    print(f"Failed to write CSV: {e}", file=sys.stderr)
    sys.exit(1)
