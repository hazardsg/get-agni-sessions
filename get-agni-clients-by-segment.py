#!/usr/bin/env python3
import requests
import os
import csv
import sys
import json
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# --- 1. Configuration ---
load_dotenv()

KEY_ID = os.getenv("KEY_ID")
KEY_VALUE = os.getenv("KEY_VALUE")
ORG_ID = os.getenv("AGNI_ORG_ID")
AGNI_URL = os.getenv("AGNI_URL")

# --- USER SETTINGS ---
TARGET_SEGMENT_NAME = "acme-wifi-employee"  # Replace with target segment name
HOURS_BACK = 24                             # Total time to look back
WINDOW_MINUTES = 30                         # Size of each query window
ENABLE_ENRICHMENT = True                    # Set to True for detailed client lookups
# ---------------------

if not KEY_ID or not KEY_VALUE or not ORG_ID or not AGNI_URL:
    print("Error: Missing required environment variables.", file=sys.stderr)
    sys.exit(1)

AGNI_URL = AGNI_URL.rstrip("/")

# --- 2. Helper Functions ---

def login(session):
    url = f"{AGNI_URL}/cvcue/keyLogin"
    try:
        resp = session.get(url, headers={"Accept": "application/json"},
                           params={"keyID": KEY_ID, "keyValue": KEY_VALUE}, timeout=30)
        resp.raise_for_status()
        print(f"Login successful to {AGNI_URL}")
    except Exception as e:
        print(f"Login failed: {e}", file=sys.stderr)
        sys.exit(1)

def get_segment_id(session, segment_name):
    """Resolve Segment Name to Segment ID."""
    url = f"{AGNI_URL}/api/config.segment.list"
    payload = {"orgID": ORG_ID}

    print(f"Looking up ID for segment: '{segment_name}'...")
    try:
        resp = session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", {})
        records = data.get("Records", [])

        for rec in records:
            if rec.get("name") == segment_name:
                seg_id = rec.get("id")
                print(f"  > Found match: ID {seg_id}")
                return seg_id

        print(f"  ! Error: Segment '{segment_name}' not found in configuration.")
        return None
    except Exception as e:
        print(f"  ! Segment lookup failed: {e}")
        return None

def get_sessions_time_sliced(session, segment_id, total_start_time):
    """
    Fetch sessions using time slices instead of cursors.
    Moves backwards from NOW to total_start_time in 30-minute increments.
    """
    url = f"{AGNI_URL}/api/session.list"

    all_sessions = []

    # Initialize windows
    current_to = datetime.now(timezone.utc)
    current_from = current_to - timedelta(minutes=WINDOW_MINUTES)

    print(f"\nScanning sessions for Segment ID {segment_id} using {WINDOW_MINUTES}m windows...")

    # Loop until we reach the start time
    while current_from >= total_start_time:
        # print(f"  > Fetching {current_from.strftime('%H:%M')} -> {current_to.strftime('%H:%M')}...")

        payload = {
            "orgID": ORG_ID,
            # Time Window
            "fromTimestamp": current_from.isoformat().replace("+00:00", "Z"),
            "toTimestamp": current_to.isoformat().replace("+00:00", "Z"),
            # Segment Filter
            "sessionType": "network_access",
            "filters": [
                {
                    "field": "segment_id",
                    "value": str(segment_id)
                }
            ],
            "limit": 1000 # Maximize limit per window
        }

        try:
            resp = session.post(url, json=payload, timeout=60)
            resp.raise_for_status()
            body = resp.json()

            data = body.get("data", {})
            records = data.get("records", [])

            if records:
                # print(f"    + Found {len(records)} records")
                all_sessions.extend(records)

        except requests.exceptions.RequestException as e:
            print(f"  ! API request failed for window {current_from}: {e}")
            # We continue to the next window instead of breaking completely

        # Move window backwards
        current_to = current_from
        current_from = current_to - timedelta(minutes=WINDOW_MINUTES)

        # Small sleep to be polite to the API
        time.sleep(0.1)

    print(f"  > Total raw sessions fetched: {len(all_sessions)}")
    return all_sessions

def get_client_details(session, mac):
    """Fetch details from identity.client.get"""
    url = f"{AGNI_URL}/api/identity.client.get"
    payload = {
        "orgID": ORG_ID,
        "mac": mac
    }
    try:
        resp = session.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("data", {})
    except Exception:
        pass
    return {}

# --- 3. Main Logic ---

session = requests.Session()
login(session)

# 1. Resolve Segment ID
segment_id = get_segment_id(session, TARGET_SEGMENT_NAME)
if not segment_id:
    sys.exit(1)

# 2. Fetch Sessions (Time Sliced)
start_limit = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
raw_sessions = get_sessions_time_sliced(session, segment_id, start_limit)

# 3. Deduplicate by MAC
unique_devices = {}
for sess in raw_sessions:
    mac = sess.get("mac")
    if not mac:
        continue
    # Overwrite ensures we keep the latest session info found in the windows
    unique_devices[mac] = sess

print(f"Found {len(unique_devices)} unique devices.")

# 4. Enrich
final_records = []

if unique_devices and ENABLE_ENRICHMENT:
    print(f"Enriching data (fetching client details)...")
    count = 0
    total = len(unique_devices)

    for mac, sess_data in unique_devices.items():
        count += 1
        if count % 5 == 0 or count == total:
            print(f"  Processed {count}/{total}...", end="\r")

        combined_rec = sess_data.copy()

        # Call Client API
        client_data = get_client_details(session, mac)

        if client_data:
            # Flatten attributes
            client_attrs = client_data.pop("attributes", {})
            if client_attrs:
                for k, v in client_attrs.items():
                    combined_rec[f"client_attr_{k}"] = v

            # Flatten Certificate
            cert_info = client_data.pop("certificate", None)
            if cert_info and isinstance(cert_info, dict):
                combined_rec["cert_issuer"] = cert_info.get("issuer")
                combined_rec["cert_expiry"] = cert_info.get("expiryDate")

            # Merge remaining fields
            for k, v in client_data.items():
                if k not in combined_rec:
                    combined_rec[k] = v
                else:
                    combined_rec[f"client_{k}"] = v

        final_records.append(combined_rec)
        time.sleep(0.05) # Rate limit protection
    print(f"\nEnrichment complete.")
else:
    final_records = list(unique_devices.values())

# 5. Export
if final_records:
    # Build header list
    all_keys = set()
    for r in final_records:
        all_keys.update(r.keys())

    sorted_keys = list(all_keys)

    # Priority columns for the CSV
    priority_cols = [
        "mac", "username", "userID", "deviceType", "description",
        "ip", "nadName", "segmentName", "location", "lastAuthAt",
        "cert_expiry"
    ]

    for col in reversed(priority_cols):
        if col in sorted_keys:
            sorted_keys.remove(col)
            sorted_keys.insert(0, col)

    filename = f"devices_{TARGET_SEGMENT_NAME.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    try:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted_keys)
            writer.writeheader()
            writer.writerows(final_records)
        print(f"\nSuccess! Exported to: {filename}")
    except IOError as e:
        print(f"Failed to write CSV: {e}", file=sys.stderr)
else:
    print("No records found to export.")