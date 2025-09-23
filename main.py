import csv
import requests
import os
from datetime import datetime

SUPABASE_URL = "https://nwzppghjzwmrpgvuknao.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im53enBwZ2hqendtcnBndnVrbmFvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc0MDg5MDEsImV4cCI6MjA2Mjk4NDkwMX0.MdewfEP7EU2RviAnT_ZBOtCHteMSmTYSHhEg5NtyqAc"
TABLE_NAME = "observations"

HEADERS = {
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type": "application/json"
}

DATA_FOLDER = "data"

def parse_dat_file(file_path):
    rows = []
    filename = os.path.basename(file_path)
    sensor_id = filename.split("_")[0]  # Ex: stars1217

    with open(file_path, "r") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if len(row) < 4:
                continue
            if row[0].startswith("#"):
                continue
            try:
                utc_datetime = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S.%f")
                local_datetime = datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%S.%f")
                enclosure_temp = float(row[4])
                rows.append({
                    "sensor_id": sensor_id,
                    "utc_datetime": utc_datetime.isoformat(),
                    "local_datetime": local_datetime.isoformat(),
                    "enclosure_temp": enclosure_temp
                })
            except Exception as e:
                print(f"❌ Erreur parsing {file_path}: {e}")
    return rows

def upload_to_supabase(data):
    BATCH_SIZE = 500
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}",
            headers=HEADERS,
            json=batch
        )
        if response.status_code != 201:
            print(f"❌ Batch {i//BATCH_SIZE + 1} : erreur upload {response.status_code} – {response.text}")
        else:
            print(f"✅ Batch {i//BATCH_SIZE + 1} : upload réussi")
