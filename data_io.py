import os
import pandas as pd
import time
import json

CONFIG_FILE = "config.json"

FEATHER_FILE = "Books_Data.feather"
CSV_FILE = "Books_Data.csv"


def load_data(folder_path):
    feather_path = os.path.join(folder_path, FEATHER_FILE)
    csv_path = os.path.join(folder_path, CSV_FILE)

    if os.path.exists(feather_path):
        try:
            print("⚡ Loading data from Feather...")
            start = time.time()
            df = pd.read_feather(feather_path)
            load_time = time.time() - start
            print(f"⏱️ Feather load time: {load_time:.3f} seconds")
            return df, load_time, "Feather"
        except Exception as e:
            print(f"❌ Failed to load Feather: {e}")

    if os.path.exists(csv_path):
        print("📄 Loading data from CSV...")
        start = time.time()
        df = pd.read_csv(csv_path)
        load_time = time.time() - start
        print(f"⏱️ CSV load time: {load_time:.3f} seconds")
        return df, load_time, "CSV"

    print("📂 No existing data found.")
    return pd.DataFrame(), 0.0, "None"



def save_data(df, folder_path):
    feather_path = os.path.join(folder_path, FEATHER_FILE)
    csv_path = os.path.join(folder_path, CSV_FILE)

    try:
        df.to_feather(feather_path)
        print("✅ Data saved to Feather.")
    except Exception as e:
        print(f"❌ Feather save failed: {e}")

    try:
        df.to_csv(csv_path, index=False)
        print("✅ Data also saved to CSV (backup).")
    except Exception as e:
        print(f"❌ CSV save failed: {e}")

def load_last_folder():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
            return config.get("last_folder", "")
    return ""

def save_last_folder(folder_path):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_folder": folder_path}, f)
