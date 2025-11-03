# Agni Session List Fetcher

This Python script fetches failed session records from the Arista Agni API and exports them to a CSV file.

---

## Prerequisites

- Python 3.10 or higher
- Git (optional, if cloning the repository)

---

## Setup

### 1. Clone the repository (if not already done)

```bash
git clone https://github.com/hazardsg/get-agni-sessions.git
cd get-agni-sessions
```

### 2. Create a Python virtual environment

It is recommended to use a virtual environment to manage dependencies:

```bash
python3 -m venv venv
```

Activate the virtual environment:

- **On macOS/Linux:**

```bash
source venv/bin/activate
```

- **On Windows (PowerShell):**

```powershell
.\venv\Scripts\Activate.ps1
```

### 3. Install dependencies

The project includes a `requirements.txt` file:

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

The script requires API credentials and an organization ID.

1. Copy the example environment file:

```bash
cp .env_example .env
```

2. Open `.env` and set your values:

```env
KEY_ID=<your_key_id>
KEY_VALUE=<your_key_value>
AGNI_ORG_ID=<your_org_id>
```


> **Note:** The Organization ID can be found by logging into AGNI and clicking your profile icon. Then click Copy Organization ID.
> **Note:** `.env` is included in `.gitignore` to keep your credentials safe.

---

## Usage

Run the script directly from your terminal:

```bash
python get-agni-sessions.py
```

The script will:

1. Log in to the Agni API using your credentials.
2. Fetch failed session records in 30-minute windows (adjustable in the script).
3. Save all collected records to a CSV file named `agni_session_list_<timestamp>.csv`.

Example output:

```
Starting fetch using 30-minute windows
Fetching sessions from 2025-10-31T12:00:00Z → 2025-10-31T12:30:00Z
  > Retrieved 42 records
Total records collected: 42
Wrote 42 records to agni_session_list_20251031_153045.csv
```

---

## Configuration Notes

- `WINDOW_MINUTES` can be adjusted in the script to control the time range per API call.
- `START_DATE` sets how far back in time to fetch session records.
- A small delay (`time.sleep(0.2)`) is included to prevent hitting API rate limits.

---

## Troubleshooting

- **Missing environment variables:** Ensure `.env` contains `KEY_ID`, `KEY_VALUE`, and `AGNI_ORG_ID`.
- **API login failures:** Check your credentials and network access to the Agni API.
- **No records exported:** Confirm the API returned records for the requested time range.

---

## License

Specify your license here (e.g., MIT, Apache 2.0).
