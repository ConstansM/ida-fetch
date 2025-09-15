import os
import requests
from datetime import datetime

# Supabase config
SUPABASE_URL = "https://nwzppghjzwmrpgvuknao.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
TABLE_NAME = "observations"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# 📁 Dossier avec les fichiers .dat
DATA_FOLDER = "data"

def parse_line(line):
    # Exemple simplifié basé sur ton format
    parts = line.strip().split()
    if len(parts) < 10:
        return None

    try:
        return {
            "utc_datetime": parts[0] + "T" + parts[1] + "+00:00",
            "local_datetime": parts[2] + "T" + parts[3],
            "enclosure_temp": float(parts[4]),
            "sky_temp": float(parts[5]),
            "frequency": float(parts[6]),
            "mssa": float(parts[7]),
            "zp": float(parts[8]),
            "sequence": int(parts[9]),
            "tx_period": 1  # valeur par défaut
        }
    except:
        return None

def send_data_to_supabase(payload):
    res = requests.post(f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}", headers=HEADERS, json=payload)
    if res.status_code == 201:
        print("✅ Données insérées avec succès")
    else:
        print(f"❌ Erreur {res.status_code} – {res.text}")

def main():
    for filename in os.listdir(DATA_FOLDER):
        if not filename.endswith(".dat"):
            continue

        filepath = os.path.join(DATA_FOLDER, filename)
        if os.stat(filepath).st_size == 0:
            continue  # fichier vide, on ignore

        print(f"📦 Parsing {filename}")
        sensor_id = filename.split("_")[0]

        with open(filepath, "r") as f:
            lines = f.readlines()

        parsed_data = []
        for line in lines:
            parsed = parse_line(line)
            if parsed:
                parsed["sensor_id"] = sensor_id
                parsed_data.append(parsed)

        if parsed_data:
            send_data_to_supabase(parsed_data)

if __name__ == "__main__":
    main()
