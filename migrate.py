import streamlit as st
import pandas as pd
from supabase import create_client, Client

# SUPABASE KEYS (Get these from Supabase Dashboard)
url: str = "irtfdgopcwwggjjegeqs"
key: str = "sb_publishable_YP4AMxiJU4D0Y4QPMy2GOg_1FIo_oi-" # Use Service Role Key to bypass RLS for migration
supabase: Client = create_client(url, key)

# LOAD YOUR EXISTING DATA FUNCTION (Copy from your old app)
# ... (paste your get_all_data() function here or load csvs) ...

def migrate_data():
    data = get_all_data() # From your old code
    
    # 1. ENTITIES
    df_ent = data.get('Entities')
    if not df_ent.empty:
        records = []
        for _, row in df_ent.iterrows():
            records.append({
                "name": row['Name'],
                "type": row['Type'],
                "fein": row.get('FEIN', ''),
                "drive_folder_id": row.get('Drive_Folder_ID', '')
            })
        supabase.table("entities").insert(records).execute()
        print(f"Migrated {len(records)} entities.")

    # 2. CONTACTS
    df_con = data.get('Contacts')
    if not df_con.empty:
        records = []
        for _, row in df_con.iterrows():
            records.append({
                "first_name": row['First Name'],
                "last_name": row['Last Name'],
                "email": row.get('Email', ''),
                "phone": row.get('Phone', '')
            })
        supabase.table("contacts").insert(records).execute()
        print(f"Migrated {len(records)} contacts.")
    
    # ... Repeat for Tasks ...

if __name__ == "__main__":
    migrate_data()
