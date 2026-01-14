#!/usr/bin/env python3
import requests
import os
import csv
import sys
import json
import time
import concurrent.futures
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from threading import Lock

# --- 1. Configuration ---
load_dotenv()

KEY_ID = os.getenv("KEY_ID")
KEY_VALUE = os.getenv("KEY_VALUE")
ORG_ID = os.getenv("AGNI_ORG_ID")
AGNI_URL = os.getenv("AGNI_URL")

# --- USER SETTINGS ---
TARGET_SEGMENT_NAME = "Default"  # Target Segment
HOURS_BACK = 24                             # Time window
ENABLE_ENRICHMENT = True                    # Must be True to get Port/Switch info
MAX_THREADS = 20                            # Number of concurrent API threads
# ---------------------

if not KEY_ID or not KEY_VALUE or not ORG_ID or not AGNI_URL:
    print("Error: Missing required environment variables.", file=sys.stderr)
    sys.exit(1)

AGNI_URL = AGNI_URL.rstrip("/")

# Shared Cache for NADs (Switch Info) to avoid re-fetching same switch 100 times
nad_cache = {}
nad_cache_lock = Lock()

# --- 2. API Helper Functions ---

def create_session():
    s = requests.Session()
    login_url = f"{AGNI_URL}/cvcue/keyLogin"
    try:
        resp = s.get(login_url, headers={"Accept": "application/json"},
                     params={"keyID": KEY_ID, "keyValue": KEY_VALUE}, timeout=30)
        resp.raise_for_status()
        return s
    except Exception as e:
        print(f"Login failed: {e}", file=sys.stderr)
        sys.exit(1)

def get_segment_id(session, segment_name):
    url = f"{AGNI_URL}/api/config.segment.list"
    try:
        resp = session.post(url, json={"orgID": ORG_ID}, timeout=30)
        resp.raise_for_status()
        records = resp.json().get("data", {}).get("Records", [])
        for rec in records:
            if rec.get("name") == segment_name:
                return rec.get("id")
        print(f"Error: Segment '{segment_name}' not found.")
        return None
    except Exception as e:
        print(f"Segment lookup failed: {e}")
        return None

def get_sessions_time_sliced(session, segment_id, total_start_time):
    url = f"{AGNI_URL}/api/session.list"
    all_sessions = []
    current_to = datetime.now(timezone.utc)
    current_from = current_to - timedelta(minutes=30)

    print(f"Scanning sessions for Segment ID {segment_id}...")

    while current_from >= total_start_time:
        payload = {
            "orgID": ORG_ID,
            "fromTimestamp": current_from.isoformat().replace("+00:00", "Z"),
            "toTimestamp": current_to.isoformat().replace("+00:00", "Z"),
            "sessionType": "network_access",
            "filters": [{"field": "segment_id", "value": str(segment_id)}],
            "limit": 1000
        }
        try:
            resp = session.post(url, json=payload, timeout=60)
            if resp.status_code == 200:
                recs = resp.json().get("data", {}).get("records", [])
                if recs: all_sessions.extend(recs)
        except Exception:
            pass # Skip failed windows to keep moving

        current_to = current_from
        current_from = current_to - timedelta(minutes=30)
        time.sleep(0.1)

    print(f"  > Total raw sessions fetched: {len(all_sessions)}")
    return all_sessions

# --- 3. Enrichment Workers ---

def get_nad_name(session, nad_id):
    """Fetch Switch Name. Thread-safe caching."""
    if not nad_id: return "Unknown"

    # Check Cache first
    with nad_cache_lock:
        if nad_id in nad_cache:
            return nad_cache[nad_id]

    # Fetch from API
    url = f"{AGNI_URL}/api/config.nad.get"
    try:
        resp = session.post(url, json={"id": nad_id, "orgID": ORG_ID}, timeout=20)
        if resp.status_code == 200:
            name = resp.json().get("data", {}).get("name", "Unknown")
            with nad_cache_lock:
                nad_cache[nad_id] = name
            return name
    except Exception:
        pass
    return "Unknown"

def get_session_details(session, auth_req_id):
    """Fetch NAS-Port-Id from session details."""
    if not auth_req_id: return ""
    url = f"{AGNI_URL}/api/session.details.get"
    try:
        resp = session.post(url, json={"authReqID": auth_req_id, "orgID": ORG_ID}, timeout=20)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            input_attrs = data.get("inputAttrs", {})
            # Look for standard NAS-Port-Id
            return input_attrs.get("Radius:IETF:NAS-Port-Id", "")
    except Exception:
        pass
    return ""

def get_client_info(session, mac):
    """Fetch extended client info."""
    url = f"{AGNI_URL}/api/identity.client.get"
    try:
        resp = session.post(url, json={"mac": mac, "orgID": ORG_ID}, timeout=20)
        if resp.status_code == 200:
            return resp.json().get("data", {})
    except Exception:
        pass
    return {}

def enrich_device_worker(args):
    """
    Worker function for ThreadPool.
    args = (mac, session_record, auth_session_obj)
    """
    mac, sess_data, auth_session = args

    combined_rec = sess_data.copy()

    # 1. Get Switch Name (Use cached NAD lookup)
    # session.list usually has 'nadName', but user requested explicit lookup
    nad_id = sess_data.get("nadID")
    if nad_id:
        combined_rec["switch_name"] = get_nad_name(auth_session, nad_id)

    # 2. Get Interface (Port ID) from Session Details
    auth_req_id = sess_data.get("authReqID")
    if auth_req_id:
        combined_rec["switch_interface"] = get_session_details(auth_session, auth_req_id)

    # 3. Get Client Identity Details
    client_data = get_client_info(auth_session, mac)
    if client_data:
        # Flatten attributes
        client_attrs = client_data.pop("attributes", {})
        if client_attrs:
            for k, v in client_attrs.items():
                combined_rec[f"client_attr_{k}"] = v

        # Flatten Certificate
        cert_info = client_data.pop("certificate", None)
        if cert_info and isinstance(cert_info, dict):
            combined_rec["cert_subject"] = cert_info.get("subject")
            combined_rec["cert_expiry"] = cert_info.get("expiryDate")

        # Merge remaining client fields (prefix collisions)
        for k, v in client_data.items():
            if k not in combined_rec:
                combined_rec[k] = v
            else:
                combined_rec[f"client_{k}"] = v

    return combined_rec

# --- 4. Main Execution ---

# Setup Session
main_session = create_session()

# Resolve Segment
seg_id = get_segment_id(main_session, TARGET_SEGMENT_NAME)
if not seg_id: sys.exit(1)

# Fetch Sessions
start_dt = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
raw_sessions = get_sessions_time_sliced(main_session, seg_id, start_dt)

# Deduplicate (Map MAC -> Latest Session)
unique_devices = {}
for sess in raw_sessions:
    m = sess.get("mac")
    if m: unique_devices[m] = sess

print(f"Found {len(unique_devices)} unique devices.")

# Enrich (Multi-threaded)
final_records = []

if ENABLE_ENRICHMENT and unique_devices:
    print(f"Enriching devices using {MAX_THREADS} threads...")

    # Create a list of arguments for the worker [ (mac, data, session), ... ]
    work_items = [(k, v, main_session) for k, v in unique_devices.items()]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Map executes the function across the list
        results = list(executor.map(enrich_device_worker, work_items))
        final_records = results

    print(f"Enrichment complete.")
else:
    final_records = list(unique_devices.values())

# Export
if final_records:
    all_keys = set().union(*(d.keys() for d in final_records))
    sorted_keys = list(all_keys)

    # Priority Columns
    priority = [
        "mac", "username", "userID", "switch_name", "switch_interface",
        "ip", "deviceType", "segmentName", "location", "cert_expiry"
    ]

    for col in reversed(priority):
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
    print("No records found.")