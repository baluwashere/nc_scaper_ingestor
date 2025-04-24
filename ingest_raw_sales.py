import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime
import uuid

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
print("✅ Connected to Supabase")

RAW_PATH = "./raw_data"

def log_etl(task, status, message):
    supabase.table("etl_log").insert({
        "task_name": task,
        "status": status,
        "message": message
    }).execute()

def load_and_clean_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns=lambda x: x.strip().lower())
        df["domain_name"] = df["domain_name"].str.lower().str.strip()
        df["date"] = pd.to_datetime(df["date"], errors='coerce')
        df["price"] = pd.to_numeric(df["price"], errors='coerce')
        df = df.dropna(subset=["domain_name", "date", "price"])
        return df
    except Exception as e:
        log_etl("load_csv", "error", f"{file_path} - {str(e)}")
        return pd.DataFrame()

def domain_exists(domain_name):
    resp = supabase.table("dn").select("id").eq("dn_name", domain_name).execute()
    return resp.data[0]["id"] if resp.data else None

def insert_new_domain(domain_name):
    domain_id = str(uuid.uuid4())
    supabase.table("dn").insert({"id": domain_id, "dn_name": domain_name, "tld": domain_name.split('.')[-1]}).execute()
    return domain_id

def process_file(file_path):
    df = load_and_clean_csv(file_path)
    inserted_sales = 0

    for _, row in df.iterrows():
        domain_id = domain_exists(row["domain_name"])
        if not domain_id:
            domain_id = insert_new_domain(row["domain_name"])

        supabase.table("sales").insert({
            "dn_id": domain_id,
            "price": row["price"],
            "date": row["date"].date().isoformat(),
            "platform": row.get("platform", None),
            "source_url": row.get("source_url", None)
        }).execute()
        inserted_sales += 1

    log_etl("process_file", "success", f"{file_path} - Inserted {inserted_sales} sales")

def main():
    for file in os.listdir(RAW_PATH):
        if file.endswith(".csv"):
            file_path = os.path.join(RAW_PATH, file)
            print(f"📂 Processing: {file_path}")
            process_file(file_path)

if __name__ == "__main__":
    main()
