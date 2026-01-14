#!/usr/bin/env python3
import requests
import os
import csv
import sys
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# --- 1. Configuration ---
load_dotenv()

KEY_ID = os.getenv("KEY_ID")
KEY_VALUE = os.getenv("KEY_VALUE")
ORG_ID = os.getenv("AGNI_ORG_ID")
AGNI_URL = os.getenv("AGNI_URL") # Example: https://beta.agni.arista.io

# Validate Environment Variables
if not KEY_ID or not KEY_VALUE or not ORG_ID or not AGNI_URL:
    print("Error: Missing required environment variables in .env", file=sys.stderr)
    print("Ensure KEY_ID, KEY_VALUE, AGNI_ORG_ID, and AGNI_URL are set.", file=sys.stderr)
    sys.exit(1)

# Ensure URL does not end with a slash for consistent path joining
AGNI_URL = AGNI_URL.rstrip("/")

# Stats types to query
STATS_TYPES = [
    "stats.count.users",
    "stats.count.clients",
    "stats.count.nads",
    "hourly.auth.count",
    "daily.topN.auth.errors",
    "daily.topN.locations.failed"
]

# Output directory
OUTPUT_DIR = "stats"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Time window
HOURS_BACK = 24

# --- 2. Login ---
session = requests.Session()
login_url = f"{AGNI_URL}/cvcue/keyLogin"

try:
    resp = session.get(login_url, headers={"Accept": "application/json"},
                       params={"keyID": KEY_ID, "keyValue": KEY_VALUE}, timeout=30)
    resp.raise_for_status()
    print(f"Login successful to {AGNI_URL}. Output directory: ./{OUTPUT_DIR}/")
except Exception as e:
    print(f"Login failed: {e}", file=sys.stderr)
    sys.exit(1)

# API Endpoint construction
API_URL = f"{AGNI_URL}/api/stats.get"
start_time_str = (datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)).isoformat().replace("+00:00", "Z")
timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")

# --- 3. Iterate through Stats Types ---
for stat_type in STATS_TYPES:
    print(f"\n--- Querying: {stat_type} ---")

    payload = {
        "orgID": ORG_ID,
        "type": stat_type,
        "from": start_time_str
    }

    try:
        resp = session.post(API_URL, json=payload, timeout=60)
        resp.raise_for_status()
        body = resp.json()

        # Check for application-level errors in the body
        if "error" in body and body["error"]:
            print(f"  ! API Error for {stat_type}: {body['error']}")
            continue

        records = body.get("data", {}).get("records", [])

        if not records:
            print(f"  > No data returned for {stat_type}")
            continue

        # --- 4. Flatten Data ---
        flattened_records = []
        all_keys = set()

        for rec in records:
            flat_rec = {}
            # Base record usually has 'dateTime'
            if "dateTime" in rec:
                flat_rec["dateTime"] = rec["dateTime"]
                all_keys.add("dateTime")

            # Extract nested 'stats' dictionary
            stats_data = rec.get("stats", {})
            if stats_data:
                for k, v in stats_data.items():
                    # Handle nested objects (like TopN often returns) by stringifying them
                    if isinstance(v, (dict, list)):
                        flat_rec[k] = json.dumps(v)
                    else:
                        flat_rec[k] = v
                    all_keys.add(k)

            # Add any other keys at the root level (excluding what we just processed)
            for k, v in rec.items():
                if k not in ["stats", "dateTime"]:
                    flat_rec[k] = v
                    all_keys.add(k)

            flattened_records.append(flat_rec)

        # --- 5. Write to CSV ---
        if flattened_records:
            # Sanitize filename
            safe_name = stat_type.replace(".", "_")
            filename = f"{OUTPUT_DIR}/{safe_name}_{timestamp_file}.csv"

            # Sort headers: dateTime first, then alphabetical
            sorted_keys = list(all_keys)
            if "dateTime" in sorted_keys:
                sorted_keys.remove("dateTime")
                sorted_keys.insert(0, "dateTime")
            else:
                sorted_keys.sort()

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=sorted_keys)
                writer.writeheader()
                writer.writerows(flattened_records)

            print(f"  > Saved {len(flattened_records)} rows to {filename}")
        else:
            print(f"  > Records found but empty content after flattening.")

    except Exception as e:
        print(f"  ! Failed to fetch {stat_type}: {e}")

print("\nBatch job complete.")