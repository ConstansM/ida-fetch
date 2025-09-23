import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def parse_dat_file(filepath, sensor_id):
    rows = []
    with open(filepath, "r") as file:
        for line in file:
            if line.startswith("#"):
                continue

            parts = line.strip().split()
            if len(parts) < 5:
                continue

            try:
                dt_str = f"{parts[0]} {parts[1]}"
                utc_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                sky_temp = float(parts[2])
                enclosure_temp = float(parts[3])
                frequency = float(parts[4])

                rows.append({
                    "sensor_id": sensor_id,
                    "utc_datetime": utc_dt.isoformat(),
                    "sky_temp": sky_temp,
                    "enclosure_temp": enclosure_temp,
                    "frequency": frequency
                })

            except Exception as e:
                print(f"⚠️ Erreur sur la ligne : {line.strip()} - {e}")
                continue

    return rows

def upload_to_supabase(data):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    response = requests.post(f"{SUPABASE_URL}/rest/v1/observations", headers=headers, json=data)
    if response.status_code not in (200, 201):
        print(f"❌ Échec de l'envoi : {response.status_code} - {response.text}")
    else:
        print(f"✅ Données envoyées : {len(data)} lignes")

if __name__ == "__main__":
    file_path = "data/stars1228_2025-09.dat"
    sensor_id = "stars1228"

    print("📦 Lecture du fichier...")
    data = parse_dat_file(file_path, sensor_id)

    print(f"📊 {len(data)} lignes parsées.")
    if data:
        print("🚀 Envoi à Supabase...")
        upload_to_supabase(data)
    else:
        print("⚠️ Aucune donnée à envoyer.")
