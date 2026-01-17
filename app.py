import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
import time
import socket
import re
import uuid
import io

# ==========================================
# 0. CONFIG & SAFETY
# ==========================================
socket.setdefaulttimeout(60)
st.set_page_config(page_title="Kohani CRM", page_icon="🏢", layout="wide")

st.markdown("""
    <style>
    #MainMenu {display: none;}
    header {visibility: hidden;}
    .stDataFrame { border: 1px solid #ddd; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 🛑 CONFIGURATION REQUIRED HERE 🛑
# ==========================================
ADMIN_EMAIL = "ali@kohani.com"

# 1. PASTE YOUR SHARED DRIVE FOLDER ID HERE 👇
ROOT_DRIVE_FOLDER_ID = "0AF0LoD230jIaUk9PVA" 

# 2. PASTE YOUR APP URL HERE 👇 (No trailing slash)
APP_BASE_URL = "https://kohani-crm.streamlit.app/" 


SCOPES_GMAIL = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile',
    'openid'
]
SCOPES_DRIVE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ==========================================
# 1. AUTHENTICATION
# ==========================================
def get_service_account_creds():
    if "connections" not in st.secrets:
        st.error("❌ Secrets missing.")
        st.stop()
    secrets_dict = dict(st.secrets["connections"]["gsheets"])
    if "private_key" in secrets_dict:
        secrets_dict["private_key"] = secrets_dict["private_key"].replace("\\n", "\n")
    return Credentials.from_service_account_info(secrets_dict, scopes=SCOPES_DRIVE)

def get_db_client():
    creds = get_service_account_creds()
    return gspread.authorize(creds)

def get_drive_service():
    creds = get_service_account_creds()
    return build('drive', 'v3', credentials=creds)

def get_auth_flow():
    return Flow.from_client_config(
        {
            "web": {
                "client_id": st.secrets["client"]["client_id"],
                "client_secret": st.secrets["client"]["client_secret"],
                "auth_uri": st.secrets["client"]["auth_uri"],
                "token_uri": st.secrets["client"]["token_uri"],
                "redirect_uris": [st.secrets["client"]["redirect_uri"]]
            }
        },
        scopes=SCOPES_GMAIL,
        redirect_uri=st.secrets["client"]["redirect_uri"]
    )

def authenticate_user():
    if "code" in st.query_params:
        try:
            code = st.query_params["code"]
            flow = get_auth_flow()
            flow.fetch_token(code=code)
            st.session_state.creds = flow.credentials
            
            user_info_service = build('oauth2', 'v2', credentials=st.session_state.creds)
            user_info = user_info_service.userinfo().get().execute()
            
            st.session_state.user_email = user_info.get('email')
            st.session_state.user_name = user_info.get('name')
            st.query_params.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Login Error: {e}")
            return False

    if "creds" in st.session_state:
        if st.session_state.creds.expired and st.session_state.creds.refresh_token:
            try:
                st.session_state.creds.refresh(Request())
            except:
                del st.session_state.creds
                return False
        return True
    return False

def get_gmail_service():
    if "creds" not in st.session_state: return None
    return build('gmail', 'v1', credentials=st.session_state.creds)

def send_email_as_user(to_email, subject, body_html):
    try:
        service = get_gmail_service()
        message = MIMEMultipart('alternative')
        sender = f"{st.session_state.user_name} <{st.session_state.user_email}>"
        message['to'] = to_email
        message['from'] = sender
        if st.session_state.user_email.lower() != ADMIN_EMAIL.lower():
            message['cc'] = ADMIN_EMAIL
        message['subject'] = subject
        part2 = MIMEText(body_html, 'html')
        message.attach(part2)
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        service.users().messages().send(userId='me', body={'raw': raw}).execute()
        return True
    except Exception as e:
        st.error(f"Gmail Error: {e}")
        return False

# ==========================================
# 2. DATABASE CORE (CACHED)
# ==========================================
@st.cache_data(ttl=60) 
def get_all_data():
    client = get_db_client()
    try:
        raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
        sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
        sh = client.open_by_key(sheet_id)
        tabs = ['Entities', 'Contacts', 'Relationships', 'Services_Settings', 
                'Services_Assigned', 'Tasks', 'App_Logs', 'Templates', 'Clients']
        data = {}
        for t in tabs:
            try:
                ws = sh.worksheet(t)
                vals = ws.get_all_values()
                if vals: data[t] = pd.DataFrame(vals[1:], columns=vals[0])
                else: data[t] = pd.DataFrame()
            except: data[t] = pd.DataFrame()
        return data
    except Exception as e:
        st.error(f"DB Load Error: {e}")
        return {}

def append_to_sheet(sheet_name, row_data):
    client = get_db_client()
    raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
    sh = client.open_by_key(raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip())
    sh.worksheet(sheet_name).append_row(row_data)
    get_all_data.clear()

def update_cell(sheet_name, row_idx, col_idx, value):
    client = get_db_client()
    raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
    sh = client.open_by_key(raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip())
    sh.worksheet(sheet_name).update_cell(row_idx, col_idx, value)
    get_all_data.clear()

# ==========================================
# 3. DRIVE & UPLOAD (SHARED DRIVE VERSION)
# ==========================================
def create_drive_folder(folder_name):
    """Creates folder INSIDE the Shared Drive Root."""
    if not ROOT_DRIVE_FOLDER_ID:
        st.error("ADMIN ERROR: ROOT_DRIVE_FOLDER_ID not set in code.")
        return None
        
    service = get_drive_service()
    
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [ROOT_DRIVE_FOLDER_ID]
    }
    
    # supportsAllDrives=True is REQUIRED for Shared Drives
    try:
        file = service.files().create(
            body=file_metadata, 
            fields='id', 
            supportsAllDrives=True
        ).execute()
        return file.get('id')
    except Exception as e:
        st.error(f"Drive Error: {e}")
        return None

def upload_file_to_drive(uploaded_file, folder_id):
    try:
        service = get_drive_service()
        file_metadata = {'name': uploaded_file.name, 'parents': [folder_id]}
        fh = io.BytesIO(uploaded_file.getvalue())
        media = MediaIoBaseUpload(fh, mimetype=uploaded_file.type, resumable=True)
        # Uploading to Shared Drive also requires supportsAllDrives=True
        service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields='id',
            supportsAllDrives=True
        ).execute()
        return True
    except: return False

def render_public_upload_portal(token):
    st.markdown("## 📤 Secure Document Upload")
    data = get_all_data()
    df_tasks = data.get('Tasks', pd.DataFrame())
    
    if df_tasks.empty or 'Upload_Token' not in df_tasks.columns:
        st.error("System error."); return

    task = df_tasks[df_tasks['Upload_Token'] == token]
    if task.empty:
        st.error("⚠️ Invalid link."); return
        
    task_row = task.iloc[0]
    entity_id = task_row['Entity_ID']
    service_name = task_row['Service_Name']
    
    df_ent = data.get('Entities')
    entity = df_ent[df_ent['ID'] == entity_id].iloc[0]
    drive_id = entity.get('Drive_Folder_ID')
    
    st.info(f"Uploading for: **{entity['Name']}** ({service_name})")
    
    if not drive_id:
        st.warning("Secure folder not set up for this client."); return

    files = st.file_uploader("Select files", accept_multiple_files=True)
    if st.button("🚀 Upload", type="primary"):
        if files:
            with st.status("Uploading..."):
                for f in files: upload_file_to_drive(f, drive_id)
            st.balloons(); st.success("✅ Success! You can close this page.")

# ==========================================
# 4. AUTOMATION
# ==========================================
def generate_id(prefix): return f"{prefix}-{str(uuid.uuid4())[:8]}"

def run_daily_automation(data, force=False):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    df_logs = data.get('App_Logs', pd.DataFrame())
    
    if not force and not df_logs.empty:
        if df_logs['Timestamp'].str.contains(today).any(): return 
            
    df_services = data.get('Services_Settings', pd.DataFrame())
    df_assigned = data.get('Services_Assigned', pd.DataFrame())
    df_tasks = data.get('Tasks', pd.DataFrame())
    new_tasks = []
    
    if not df_services.empty and not df_assigned.empty:
        for _, assign in df_assigned.iterrows():
            s_name = assign['Service_Name']; e_id = assign['Entity_ID']
            rule = df_services[df_services['Service_Name'] == s_name]
            if not rule.empty:
                freq = rule.iloc[0]['Frequency']
                due_day = int(rule.iloc[0].get('Due_Rule_Days', 15))
                now = datetime.datetime.now()
                due_str = ""

                if freq == "Monthly":
                    nm = now.month + 1 if now.month < 12 else 1
                    ny = now.year if now.month < 12 else now.year + 1
                    due_str = f"{ny}-{nm:02d}-{due_day:02d}"
                elif freq == "Quarterly":
                    due_str = (now + datetime.timedelta(days=30)).strftime("%Y-%m-%d")
                elif freq == "Annually":
                    tm = 3 if any(x in s_name for x in ["1120-S", "1065", "Partnership", "S-Corp"]) else 4
                    deadline = datetime.datetime(now.year, tm, due_day)
                    due_str = deadline.strftime("%Y-%m-%d") if now < deadline else f"{now.year+1}-{tm:02d}-{due_day:02d}"
                elif freq == "One-Time":
                    due_str = (now + datetime.timedelta(days=15)).strftime("%Y-%m-%d")

                if due_str:
                    dup = False
                    if not df_tasks.empty:
                        mask = (df_tasks['Entity_ID'] == e_id) & (df_tasks['Service_Name'] == s_name) & (df_tasks['Due_Date'] == due_str)
                        if not df_tasks[mask].empty: dup = True
                    if not dup:
                        new_tasks.append([generate_id("T"), e_id, s_name, due_str, "Not Started", str(uuid.uuid4()), "", ""])
    
    if new_tasks:
        for t in new_tasks: append_to_sheet("Tasks", t)
        append_to_sheet("App_Logs", [datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "Generated Tasks", f"Count: {len(new_tasks)}"])
        st.toast(f"Generated {len(new_tasks)} tasks")
    else:
        if force: st.toast("No new tasks.")
        append_to_sheet("App_Logs", [datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "Daily Check", "No new tasks"])

# ==========================================
# 5. UI: MIGRATION
# ==========================================
def render_migration_tool(data):
    st.subheader("Migration")
    df_old = data.get('Clients')
    if not df_old.empty:
        st.write(f"Found {len(df_old)} legacy rows.")
        if st.button("🚀 Run Batch Migration"):
            batch_e = []; batch_c = []; batch_r = []
            progress = st.progress(0)
            for i, row in df_old.iterrows():
                eid = generate_id("E")
                nm = row.get('Name') or "Unknown"
                batch_e.append([eid, nm, "Unknown", "", "", "", ""])
                cid = generate_id("C")
                tp_n = row.get('Taxpayer First Name') or nm
                batch_c.append([
                    cid, tp_n, row.get('Taxpayer last name', ''), 
                    row.get('Taxpayer E-mail Address', ''), 
                    row.get('Home Telephone', ''), "Taxpayer", "", 
                    row.get('Status', 'New'), row.get('Outcome', ''), 
                    row.get('Last_Agent', ''), row.get('Last_Updated', '')
                ])
                batch_r.append([eid, cid, "Owner", "100%"])
                if row.get('Spouse First Name'):
                    cid2 = generate_id("C")
                    batch_c.append([
                        cid2, row.get('Spouse First Name'), row.get('Spouse last name', ''),
                        row.get('Spouse E-mail Address', ''), "", "Spouse", "", "New", "", "", ""
                    ])
                    batch_r.append([eid, cid2, "Spouse", ""])
            
            cl = get_db_client()
            sh = cl.open_by_key(st.secrets["connections"]["gsheets"]["spreadsheet"].split("/")[5])
            if batch_e: sh.worksheet("Entities").append_rows(batch_e)
            if batch_c: sh.worksheet("Contacts").append_rows(batch_c)
            if batch_r: sh.worksheet("Relationships").append_rows(batch_r)
            st.success("Done!"); time.sleep(2); st.rerun()

# ==========================================
# 6. MAIN APP
# ==========================================
if "upload_token" in st.query_params:
    render_public_upload_portal(st.query_params["upload_token"])
    st.stop()

if not authenticate_user():
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        st.title("Kohani Practice Management")
        flow = get_auth_flow()
        auth_url, _ = flow.authorization_url(prompt='consent')
        st.link_button("🔵 Sign in with Google", auth_url, type="primary")
else:
    data_dict = get_all_data()
    run_daily_automation(data_dict) 

    with st.sidebar:
        st.write(f"👤 **{st.session_state.user_name}**")
        nav = st.radio("Navigation", ["📊 Dashboard", "📞 Call Queue", "🏢 Entities", "👥 Contacts", "✅ Production (Tasks)", "🔒 Admin"])
        if st.button("Logout"):
            del st.session_state.creds
            st.rerun()

    # --- DASHBOARD ---
    if nav == "📊 Dashboard":
        st.title("Practice Dashboard")
        df_tasks = data_dict.get('Tasks', pd.DataFrame())
        df_ent = data_dict.get('Entities', pd.DataFrame())
        c1, c2, c3 = st.columns(3)
        c1.metric("Active Entities", len(df_ent) if not df_ent.empty else 0)
        pending = len(df_tasks[df_tasks['Status'] != 'Done']) if not df_tasks.empty else 0
        c2.metric("Pending Tasks", pending)
        c3.metric("System Status", "Online 🟢")

    # --- CALL QUEUE ---
    elif nav == "📞 Call Queue":
        st.title("📞 Call Queue & CRM")
        df_con = data_dict.get('Contacts', pd.DataFrame())
        if df_con.empty:
            st.warning("No contacts.")
        else:
            if st.session_state.get('call_id') is None:
                c1, c2 = st.columns(2)
                with c1:
                    queue = df_con[df_con['Status'].isin(['New', 'Follow Up', 'Left Message'])] if 'Status' in df_con.columns else pd.DataFrame()
                    st.write(f"**{len(queue)}** in queue")
                    if st.button("🎲 START CALL"):
                        if not queue.empty:
                            st.session_state.call_id = queue.sample(1).iloc[0]['ID']
                            st.rerun()
                with c2:
                    q = st.text_input("Search Contact")
                    if q:
                        res = df_con[df_con['First Name'].str.contains(q, case=False, na=False) | df_con['Phone'].str.contains(q, na=False)]
                        for _, r in res.iterrows():
                            if st.button(f"Load: {r['First Name']} {r['Last Name']}", key=r['ID']):
                                st.session_state.call_id = r['ID']; st.rerun()
            else:
                cid = st.session_state.call_id
                row = df_con[df_con['ID'] == cid].iloc[0]
                idx = df_con.index[df_con['ID'] == cid][0] + 2
                with st.container(border=True):
                    st.subheader(f"{row['First Name']} {row['Last Name']}")
                    if st.button("Exit"): st.session_state.call_id = None; st.rerun()
                    c1, c2 = st.columns(2)
                    ph = c1.text_input("Phone", row.get('Phone'))
                    em = c2.text_input("Email", row.get('Email'))
                    st.text_area("History", row.get('Notes', ''), disabled=True, height=100)
                    note = st.text_area("New Note")
                    s1, s2 = st.columns(2)
                    stat = s1.selectbox("Status", ["New", "Left Message", "Talked", "Wrong Number"], index=0)
                    out = s2.selectbox("Outcome", ["Pending", "Yes", "No"], index=0)
                    if st.button("Save"):
                        update_cell("Contacts", idx, 5, ph)
                        update_cell("Contacts", idx, 4, em)
                        update_cell("Contacts", idx, 8, stat)
                        update_cell("Contacts", idx, 9, out)
                        update_cell("Contacts", idx, 10, st.session_state.user_email)
                        update_cell("Contacts", idx, 11, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
                        if note: update_cell("Contacts", idx, 7, f"{row.get('Notes','')}\n[{datetime.datetime.now()}] {note}")
                        st.session_state.call_id = None; st.rerun()

    # --- ENTITIES ---
    elif nav == "🏢 Entities":
        st.title("Entity Manager")
        df_ent = data_dict.get('Entities')
        df_rel = data_dict.get('Relationships')
        df_con = data_dict.get('Contacts')
        
        c1, c2 = st.columns([1, 2])
        with c1:
            q = st.text_input("Search Entity")
            if st.button("➕ New"): st.session_state.new_ent = True
            if q and not df_ent.empty:
                disp = df_ent[df_ent['Name'].str.contains(q, case=False, na=False)]
                for _, r in disp.head(10).iterrows():
                    if st.button(f"{r['Name']}", key=f"e_{r['ID']}"): st.session_state.ent_id = r['ID']
        
        with c2:
            if st.session_state.get('new_ent'):
                st.subheader("Create Entity")
                n_nm = st.text_input("Name")
                n_tp = st.selectbox("Type", ["Individual", "LLC", "S-Corp"])
                if st.button("Create"):
                    append_to_sheet("Entities", [generate_id("E"), n_nm, n_tp, "", "", "", ""])
                    st.session_state.new_ent = False; st.rerun()
            elif st.session_state.get('ent_id'):
                eid = st.session_state.ent_id
                ent = df_ent[df_ent['ID'] == eid].iloc[0]
                st.header(ent['Name'])
                
                t1, t2, t3 = st.tabs(["Info & Drive", "People", "Services"])
                with t1:
                    e_idx = df_ent.index[df_ent['ID'] == eid][0] + 2
                    
                    # --- TYPE FIX & SAFETY ---
                    types = ["Individual", "LLC", "S-Corp", "C-Corp", "Partnership", "Non-Profit", "Trust", "Unknown"]
                    curr_type = ent.get('Type', 'Individual')
                    if curr_type not in types: curr_type = "Unknown"
                    nt = st.selectbox("Type", types, index=types.index(curr_type))
                    
                    nf = st.text_input("FEIN", ent.get('FEIN', ''))
                    if st.button("Update Info"):
                        update_cell("Entities", e_idx, 3, nt)
                        update_cell("Entities", e_idx, 4, nf)
                        st.rerun()
                    
                    # --- DRIVE LOGIC (WITH RESET) ---
                    did = ent.get('Drive_Folder_ID')
                    if did and len(str(did)) > 5:
                        st.success(f"Connected: {did}")
                        st.markdown(f"[Open Drive Folder](https://drive.google.com/drive/u/0/folders/{did})")
                        if st.button("❌ Unlink/Reset Folder"):
                            update_cell("Entities", e_idx, 7, "")
                            st.rerun()
                    else:
                        if st.button("📂 Create Shared Folder"):
                            fid = create_drive_folder(f"{ent['Name']} - {eid}")
                            if fid:
                                update_cell("Entities", e_idx, 7, fid)
                                st.rerun()

                with t2:
                    if not df_rel.empty:
                        for _, r in df_rel[df_rel['Entity_ID'] == eid].iterrows():
                            c = df_con[df_con['ID'] == r['Contact_ID']]
                            if not c.empty: st.write(f"👤 {c.iloc[0]['First Name']} ({r['Role']})")
                with t3:
                    df_set = data_dict.get('Services_Settings')
                    new_s = st.selectbox("Assign Service", df_set['Service_Name'].unique()) if not df_set.empty else None
                    if st.button("Add"):
                        append_to_sheet("Services_Assigned", [eid, new_s, datetime.datetime.now().strftime("%Y-%m-%d"), ""])
                        st.rerun()
                    curr = data_dict.get('Services_Assigned')
                    if not curr.empty: st.dataframe(curr[curr['Entity_ID'] == eid][['Service_Name']])

    # --- CONTACTS (SEARCH ONLY) ---
    elif nav == "👥 Contacts":
        st.title("Contacts Directory")
        st.info("🔒 Secure Search Mode")
        q = st.text_input("Search by Name or Phone")
        if q:
            df_con = data_dict.get('Contacts', pd.DataFrame())
            if not df_con.empty:
                res = df_con[df_con['First Name'].str.contains(q, case=False, na=False) | df_con['Phone'].str.contains(q, na=False)]
                st.dataframe(res[['First Name', 'Last Name', 'Email', 'Phone', 'Type']])

    # --- PRODUCTION ---
    elif nav == "✅ Production (Tasks)":
        st.title("Production Calendar")
        df_tasks = data_dict.get('Tasks', pd.DataFrame())
        df_ent = data_dict.get('Entities', pd.DataFrame())
        df_rel = data_dict.get('Relationships', pd.DataFrame())
        df_con = data_dict.get('Contacts', pd.DataFrame())
        
        if df_tasks.empty:
            st.info("No tasks.")
        else:
            all_stats = sorted(list(df_tasks['Status'].unique()))
            f_s = st.multiselect("Filter Status", all_stats, default=[s for s in ["Not Started", "In Progress"] if s in all_stats])
            df_full = df_tasks.merge(df_ent[['ID', 'Name']], left_on='Entity_ID', right_on='ID', how='left')
            view = df_full[df_full['Status'].isin(f_s)] if f_s else df_full
            
            for _, r in view.iterrows():
                with st.expander(f"📅 {r['Due_Date']} | {r['Name']} | {r['Service_Name']}"):
                    c1, c2, c3 = st.columns([1, 1, 2])
                    with c1:
                        ns = st.selectbox("Status", ["Not Started", "In Progress", "Done"], index=["Not Started", "In Progress", "Done"].index(r['Status']), key=f"s_{r['Task_ID']}")
                        if ns != r['Status']:
                            t_idx = df_tasks.index[df_tasks['Task_ID'] == r['Task_ID']][0] + 2
                            update_cell("Tasks", t_idx, 5, ns); st.rerun()
                    with c2:
                        link = f"{APP_BASE_URL}/?upload_token={r['Upload_Token']}"
                        st.text_input("Upload Link", link, key=f"lk_{r['Task_ID']}")
                    with c3:
                        tgt_em = None
                        if not df_rel.empty:
                            rels = df_rel[df_rel['Entity_ID'] == r['Entity_ID']]
                            for _, rel in rels.iterrows():
                                con = df_con[df_con['ID'] == rel['Contact_ID']]
                                if not con.empty and "@" in str(con.iloc[0].get('Email', '')):
                                    tgt_em = con.iloc[0]['Email']
                                    break
                        if tgt_em:
                            if st.button(f"✉️ Send to {tgt_em}", key=f"em_{r['Task_ID']}"):
                                subj = f"Action Required: {r['Service_Name']}"
                                body = f"<p>Please upload documents here:</p><p><a href='{link}'>{link}</a></p>"
                                if send_email_as_user(tgt_em, subj, body): st.toast("Sent!")
                        else:
                            st.warning("No contact email found.")

    # --- ADMIN ---
    elif nav == "🔒 Admin":
        st.title("Admin")
        t1, t2, t3 = st.tabs(["Migration", "Services", "Automation"])
        with t1: render_migration_tool(data_dict)
        with t2: st.dataframe(data_dict.get('Services_Settings'))
        with t3:
            if st.button("⚡ Force Automation"): run_daily_automation(data_dict, force=True)
