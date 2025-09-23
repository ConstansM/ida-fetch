from main import parse_dat_file, upload_to_supabase
import os

DATA_FOLDER = "data"

all_data = []

for file_name in os.listdir(DATA_FOLDER):
    if file_name.endswith(".dat"):
        print(f"📦 Parsing {file_name}")
        file_path = os.path.join(DATA_FOLDER, file_name)
        parsed_data = parse_dat_file(file_path)
        all_data.extend(parsed_data)

upload_to_supabase(all_data)
