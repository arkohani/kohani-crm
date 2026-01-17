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
st.set_page_config(page_title="Kohani CRM & Practice Management", page_icon="🏢", layout="wide")

st.markdown("""
    <style>
    #MainMenu {display: none;}
    header {visibility: hidden;}
    .stDataFrame { border: 1px solid #ddd; border-radius: 5px; }
    .status-badge { padding: 4px 8px; border-radius: 4px; font-weight: bold; color: white; }
    </style>
    """, unsafe_allow_html=True)

# CONSTANTS
ADMIN_EMAIL = "ali@kohani.com"
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
# 1. AUTHENTICATION (DUAL MODE)
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

# ==========================================
# 2. DATABASE CORE (CACHED)
# ==========================================
@st.cache_data(ttl=60) 
def get_all_data():
    """Reads ALL relevant sheets into a dictionary of DataFrames."""
    client = get_db_client()
    try:
        raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
        sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
        sh = client.open_by_key(sheet_id)
        
        # Define required tabs
        tabs = ['Entities', 'Contacts', 'Relationships', 'Services_Settings', 
                'Services_Assigned', 'Tasks', 'App_Logs', 'Templates', 'Reference', 'Clients']
        
        data = {}
        for t in tabs:
            try:
                ws = sh.worksheet(t)
                vals = ws.get_all_values()
                if vals:
                    data[t] = pd.DataFrame(vals[1:], columns=vals[0])
                else:
                    data[t] = pd.DataFrame()
            except:
                data[t] = pd.DataFrame()
        return data
    except Exception as e:
        st.error(f"DB Load Error: {e}")
        return {}

def append_to_sheet(sheet_name, row_data):
    """Appends a list of values as a new row."""
    client = get_db_client()
    raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
    sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(sheet_name)
    ws.append_row(row_data)
    get_all_data.clear()

def update_cell(sheet_name, row_idx, col_idx, value):
    """Update single cell (1-based index)."""
    client = get_db_client()
    raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
    sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(sheet_name)
    ws.update_cell(row_idx, col_idx, value)
    get_all_data.clear()

# ==========================================
# 3. DRIVE & UPLOAD
# ==========================================
def create_drive_folder(folder_name, parent_id=None):
    service = get_drive_service()
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
        
    file = service.files().create(body=file_metadata, fields='id').execute()
    return file.get('id')

def upload_file_to_drive(uploaded_file, folder_id):
    try:
        service = get_drive_service()
        file_metadata = {'name': uploaded_file.name, 'parents': [folder_id]}
        fh = io.BytesIO(uploaded_file.getvalue())
        media = MediaIoBaseUpload(fh, mimetype=uploaded_file.type, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')
    except Exception as e:
        return None

def render_public_upload_portal(token):
    st.markdown("## 📤 Secure Document Upload")
    data = get_all_data()
    df_tasks = data.get('Tasks', pd.DataFrame())
    
    if df_tasks.empty or 'Upload_Token' not in df_tasks.columns:
        st.error("System configuration error.")
        return

    task = df_tasks[df_tasks['Upload_Token'] == token]
    if task.empty:
        st.error("⚠️ Invalid or expired link.")
        return
        
    task_row = task.iloc[0]
    entity_id = task_row['Entity_ID']
    service_name = task_row['Service_Name']
    
    df_ent = data.get('Entities')
    entity = df_ent[df_ent['ID'] == entity_id].iloc[0]
    
    st.success(f"Upload documents for: **{entity['Name']}** ({service_name})")
    
    drive_id = entity.get('Drive_Folder_ID')
    if not drive_id:
        st.warning("Secure folder not set up. Please contact office.")
        return

    files = st.file_uploader("Drag and drop files here", accept_multiple_files=True)
    if st.button("🚀 Upload Files", type="primary"):
        if files:
            with st.status("Uploading..."):
                for f in files:
                    upload_file_to_drive(f, drive_id)
            st.balloons()
            st.success("✅ Files uploaded!")

# ==========================================
# 4. AUTOMATION & LOGIC (FIXED)
# ==========================================
def generate_id(prefix):
    return f"{prefix}-{str(uuid.uuid4())[:8]}"

def run_daily_automation(data, force=False):
    """Logic Engine for Tasks"""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    df_logs = data.get('App_Logs', pd.DataFrame())
    
    if not force and not df_logs.empty:
        if df_logs['Timestamp'].str.contains(today).any():
            return 
            
    df_services = data.get('Services_Settings', pd.DataFrame())
    df_assigned = data.get('Services_Assigned', pd.DataFrame())
    df_tasks = data.get('Tasks', pd.DataFrame())
    
    new_tasks = []
    
    if not df_services.empty and not df_assigned.empty:
        for _, assign in df_assigned.iterrows():
            s_name = assign['Service_Name']
            e_id = assign['Entity_ID']
            
            # Find Rule
            rule_row = df_services[df_services['Service_Name'] == s_name]
            if not rule_row.empty:
                rule = rule_row.iloc[0]
                freq = rule['Frequency']
                
                # --- INTELLIGENT DUE DATE LOGIC ---
                now = datetime.datetime.now()
                due_date_str = ""
                
                # Check explicit Day rule, default to 15
                try:
                    due_day = int(rule.get('Due_Rule_Days', 15))
                except:
                    due_day = 15

                if freq == "Monthly":
                    # Due next month
                    next_month = now.month + 1 if now.month < 12 else 1
                    year = now.year if now.month < 12 else now.year + 1
                    due_date_str = f"{year}-{next_month:02d}-{due_day:02d}"

                elif freq == "Quarterly":
                    # Q1=Apr, Q2=Jul, Q3=Oct, Q4=Jan
                    q_month = ((now.month - 1) // 3 + 1) * 3 + 1 # End of Q + 1 month
                    if q_month > 12: q_month = 1; # Logic implies next year for Q4
                    
                    # This generates the *current* relevant quarter due date
                    # For simplicity in this demo, let's look 30 days ahead
                    target_date = now + datetime.timedelta(days=30)
                    due_date_str = target_date.strftime("%Y-%m-%d")

                elif freq == "Annually":
                    # TAX LOGIC: 1120S/1065 = March 15, 1040/1120 = April 15
                    target_month = 4
                    if "1120-S" in s_name or "1065" in s_name or "S-Corp" in s_name or "Partnership" in s_name:
                        target_month = 3
                    elif "1040" in s_name or "Individual" in s_name or "C-Corp" in s_name:
                        target_month = 4
                    else:
                        target_month = 4
                    
                    # If we are before the deadline, it's due this year. If after, due next year.
                    this_year_deadline = datetime.datetime(now.year, target_month, due_day)
                    if now < this_year_deadline:
                        due_date_str = this_year_deadline.strftime("%Y-%m-%d")
                    else:
                        due_date_str = f"{now.year + 1}-{target_month:02d}-{due_day:02d}"

                elif freq == "One-Time":
                    # Due 15 days from now if not set
                    due_date_str = (now + datetime.timedelta(days=15)).strftime("%Y-%m-%d")

                # --- DUPLICATE CHECK ---
                if due_date_str:
                    duplicate = False
                    if not df_tasks.empty:
                        # Check Task for same Entity + Service + Due Date
                        mask = (df_tasks['Entity_ID'] == e_id) & \
                               (df_tasks['Service_Name'] == s_name) & \
                               (df_tasks['Due_Date'] == due_date_str)
                        if not df_tasks[mask].empty:
                            duplicate = True
                    
                    if not duplicate:
                        t_id = generate_id("T")
                        token = str(uuid.uuid4())
                        new_tasks.append([t_id, e_id, s_name, due_date_str, "Not Started", token, "", ""])
    
    if new_tasks:
        for t in new_tasks:
            append_to_sheet("Tasks", t)
        append_to_sheet("App_Logs", [datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "Generated Tasks", f"Count: {len(new_tasks)}"])
        st.toast(f"✅ Generated {len(new_tasks)} new tasks!")
    else:
        if force: st.toast("No new tasks required.")
        append_to_sheet("App_Logs", [datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "Daily Check", "No new tasks"])

# ==========================================
# 5. UI: MIGRATION (BATCH)
# ==========================================
def render_migration_tool(data):
    st.subheader("🛠️ Data Migration Tool")
    df_old = data.get('Clients')
    if df_old.empty: st.info("No legacy data."); return

    st.write(f"Found {len(df_old)} rows in Legacy Clients.")
    
    if st.button("🚀 RUN BATCH MIGRATION"):
        batch_entities = []
        batch_contacts = []
        batch_relationships = []
        progress = st.progress(0)
        
        for i, row in df_old.iterrows():
            ent_name = row.get('Name') or "Unknown"
            ent_id = generate_id("E")
            batch_entities.append([ent_id, ent_name, "Unknown", "", "", "", ""])
            
            # CRM Columns
            status = row.get('Status', 'New')
            outcome = row.get('Outcome', '')
            last_agent = row.get('Last_Agent', '')
            last_updated = row.get('Last_Updated', '')
            
            tp_first = row.get('Taxpayer First Name')
            tp_email = row.get('Taxpayer E-mail Address')
            c_name_first = tp_first if tp_first else ent_name
            c_id = generate_id("C")
            
            batch_contacts.append([
                c_id, c_name_first, row.get('Taxpayer last name', ''), 
                str(tp_email) if tp_email else "", 
                str(row.get('Home Telephone', '')) if row.get('Home Telephone') else "", 
                "Taxpayer", "", status, outcome, last_agent, last_updated
            ])
            batch_relationships.append([ent_id, c_id, "Owner", "100%"])
            
            sp_first = row.get('Spouse First Name')
            if sp_first:
                c_id_sp = generate_id("C")
                batch_contacts.append([
                    c_id_sp, sp_first, row.get('Spouse last name', ''), 
                    str(row.get('Spouse E-mail Address', '')) if row.get('Spouse E-mail Address') else "", 
                    "", "Spouse", "", "New", "", "", ""
                ])
                batch_relationships.append([ent_id, c_id_sp, "Spouse", ""])
        
        progress.progress(50)
        client = get_db_client()
        raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
        sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
        sh = client.open_by_key(sheet_id)
        
        try:
            if batch_entities: sh.worksheet("Entities").append_rows(batch_entities)
            if batch_contacts: sh.worksheet("Contacts").append_rows(batch_contacts)
            if batch_relationships: sh.worksheet("Relationships").append_rows(batch_relationships)
            progress.progress(100)
            st.balloons()
            st.success("Migration Complete!")
            time.sleep(2); st.rerun()
        except Exception as e:
            st.error(f"Save Error: {e}")

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
        
        pending_count = 0
        if not df_tasks.empty:
            pending_count = len(df_tasks[df_tasks['Status'] != 'Done'])
        c2.metric("Pending Tasks", pending_count)
        c3.metric("System Status", "Online 🟢")

    # --- CALL QUEUE (FIXED) ---
    elif nav == "📞 Call Queue":
        st.title("📞 Call Queue & CRM")
        df_con = data_dict.get('Contacts', pd.DataFrame())
        
        # SAFETY CHECK
        if df_con.empty:
            st.warning("No contacts found. Please run migration in Admin.")
        else:
            # Fix KeyError for Last_Agent if column missing
            if 'Last_Agent' not in df_con.columns: df_con['Last_Agent'] = ""
            if 'Last_Updated' not in df_con.columns: df_con['Last_Updated'] = ""
            if 'Status' not in df_con.columns: df_con['Status'] = "New"

            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            daily = df_con[df_con['Last_Updated'].str.contains(today_str, na=False)]
            my_calls = len(daily[daily['Last_Agent'] == st.session_state.user_email])
            st.metric("🔥 My Calls Today", my_calls)
            
            if "call_session_id" not in st.session_state: st.session_state.call_session_id = None
            
            if st.session_state.call_session_id is None:
                c1, c2 = st.columns([1, 1])
                with c1:
                    st.subheader("Action Required")
                    queue = df_con[df_con['Status'].isin(['New', 'Follow Up', 'Left Message'])]
                    st.write(f"**{len(queue)}** contacts in queue.")
                    if st.button("🎲 START NEXT CALL", type="primary", use_container_width=True):
                        if not queue.empty:
                            selected = queue.sample(1).iloc[0]
                            st.session_state.call_session_id = selected['ID']
                            st.rerun()
                with c2:
                    st.subheader("Search")
                    search_q = st.text_input("Find Contact")
                    if search_q:
                        res = df_con[df_con['First Name'].astype(str).str.contains(search_q, case=False) | 
                                     df_con['Phone'].astype(str).str.contains(search_q, na=False)]
                        for _, r in res.iterrows():
                            if st.button(f"Load: {r['First Name']} {r['Last Name']}", key=r['ID']):
                                st.session_state.call_session_id = r['ID']
                                st.rerun()
            else:
                # CARD VIEW
                cid = st.session_state.call_session_id
                contact = df_con[df_con['ID'] == cid].iloc[0]
                with st.container(border=True):
                    st.header(f"{contact['First Name']} {contact['Last Name']}")
                    if st.button("❌ Exit"):
                        st.session_state.call_session_id = None; st.rerun()
                    
                    c1, c2 = st.columns(2)
                    n_ph = c1.text_input("Phone", contact.get('Phone'))
                    n_em = c2.text_input("Email", contact.get('Email'))
                    
                    st.info(f"Status: {contact.get('Status')}")
                    notes_hist = str(contact.get('Notes', ''))
                    st.text_area("History", notes_hist, disabled=True, height=100)
                    new_note = st.text_area("New Note")
                    
                    r1, r2 = st.columns(2)
                    n_stat = r1.selectbox("Status", ["New", "Left Message", "Talked", "Wrong Number", "Not Interested", "Sold/Won"])
                    n_out = r2.selectbox("Outcome", ["Pending", "Yes", "No", "Maybe"])
                    
                    if st.button("💾 SAVE", type="primary"):
                        idx = df_con.index[df_con['ID'] == cid][0] + 2
                        # Assuming Order: ID, First, Last, Email, Phone, Type, Notes, Status, Outcome, Agent, Updated
                        update_cell("Contacts", idx, 5, n_ph)
                        update_cell("Contacts", idx, 4, n_em)
                        update_cell("Contacts", idx, 8, n_stat)
                        update_cell("Contacts", idx, 9, n_out)
                        update_cell("Contacts", idx, 10, st.session_state.user_email)
                        update_cell("Contacts", idx, 11, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
                        if new_note:
                            final_n = f"{notes_hist}\n[{datetime.datetime.now()}] {new_note}"
                            update_cell("Contacts", idx, 7, final_n)
                        st.success("Saved"); time.sleep(1); st.session_state.call_session_id = None; st.rerun()

    # --- ENTITIES (UPDATED EDIT) ---
    elif nav == "🏢 Entities":
        st.title("Entity Manager")
        df_ent = data_dict.get('Entities')
        df_rel = data_dict.get('Relationships')
        df_con = data_dict.get('Contacts')
        
        c_list, c_det = st.columns([1, 2])
        sel_id = None
        
        with c_list:
            search = st.text_input("Search Entity")
            if st.button("➕ New Entity"): st.session_state.new_ent = True
            
            disp = df_ent
            if search and not df_ent.empty:
                disp = df_ent[df_ent['Name'].str.contains(search, case=False, na=False)]
            
            if not disp.empty:
                sel = st.dataframe(disp[['Name', 'Type']], on_select="rerun", selection_mode="single-row")
                if len(sel.selection.rows) > 0:
                    sel_id = disp.iloc[sel.selection.rows[0]]['ID']

        with c_det:
            if st.session_state.get('new_ent'):
                st.subheader("New Entity")
                n_name = st.text_input("Name")
                n_type = st.selectbox("Type", ["Individual", "LLC", "S-Corp", "C-Corp", "Partnership", "Non-Profit"])
                if st.button("Save"):
                    nid = generate_id("E")
                    append_to_sheet("Entities", [nid, n_name, n_type, "", "", "", ""])
                    st.session_state.new_ent = False; st.rerun()
            elif sel_id:
                ent = df_ent[df_ent['ID'] == sel_id].iloc[0]
                st.header(ent['Name'])
                
                t1, t2, t3 = st.tabs(["Details", "People", "Services"])
                with t1:
                    # EDIT MODE
                    types = ["Individual", "LLC", "S-Corp", "C-Corp", "Partnership", "Non-Profit", "Unknown"]
                    curr_type = ent['Type'] if ent['Type'] in types else "Unknown"
                    
                    e_type = st.selectbox("Type", types, index=types.index(curr_type))
                    e_fein = st.text_input("FEIN", ent.get('FEIN', ''))
                    
                    if st.button("Update Details"):
                        raw_idx = df_ent.index[df_ent['ID'] == sel_id][0] + 2
                        update_cell("Entities", raw_idx, 3, e_type)
                        update_cell("Entities", raw_idx, 4, e_fein)
                        st.success("Updated!")
                        time.sleep(1); st.rerun()

                    # DRIVE
                    did = ent.get('Drive_Folder_ID')
                    if not did:
                        if st.button("📂 Create Drive Folder"):
                            fid = create_drive_folder(f"{ent['Name']} - {sel_id}")
                            raw_idx = df_ent.index[df_ent['ID'] == sel_id][0] + 2
                            update_cell("Entities", raw_idx, 7, fid) # Col 7 is Drive ID
                            st.rerun()
                    else:
                        st.success(f"Linked: {did}")

                with t2:
                    if not df_rel.empty:
                        my_rels = df_rel[df_rel['Entity_ID'] == sel_id]
                        for _, r in my_rels.iterrows():
                            c = df_con[df_con['ID'] == r['Contact_ID']]
                            if not c.empty:
                                st.write(f"👤 {c.iloc[0]['First Name']} {c.iloc[0]['Last Name']} ({r['Role']})")
                
                with t3:
                    df_assign = data_dict.get('Services_Assigned')
                    if not df_assign.empty:
                        st.dataframe(df_assign[df_assign['Entity_ID'] == sel_id][['Service_Name']])
                    
                    df_set = data_dict.get('Services_Settings')
                    if not df_set.empty:
                        add_s = st.selectbox("Add Service", df_set['Service_Name'].unique())
                        if st.button("Add Service"):
                            append_to_sheet("Services_Assigned", [sel_id, add_s, datetime.datetime.now().strftime("%Y-%m-%d"), ""])
                            st.rerun()

    # --- CONTACTS (RESTORED) ---
    elif nav == "👥 Contacts":
        st.title("Contacts Directory")
        df_con = data_dict.get('Contacts', pd.DataFrame())
        if df_con.empty:
            st.warning("No contacts found.")
        else:
            st.dataframe(df_con[['First Name', 'Last Name', 'Phone', 'Email', 'Type', 'Status']], use_container_width=True)

    # --- PRODUCTION / TASKS ---
    elif nav == "✅ Production (Tasks)":
        st.title("Production Calendar")
        df_tasks = data_dict.get('Tasks', pd.DataFrame())
        df_ent = data_dict.get('Entities', pd.DataFrame())
        
        if df_tasks.empty:
            st.info("No tasks generated yet. Go to Admin -> Force Run Automation.")
        else:
            df_full = df_tasks.merge(df_ent[['ID', 'Name']], left_on='Entity_ID', right_on='ID', how='left')
            f_stat = st.multiselect("Filter Status", df_tasks['Status'].unique(), default=["Not Started", "In Progress"])
            view = df_full[df_full['Status'].isin(f_stat)] if f_stat else df_full
            
            for _, row in view.iterrows():
                with st.expander(f"📅 {row['Due_Date']} | {row['Name']} | {row['Service_Name']}"):
                    c1, c2 = st.columns(2)
                    with c1:
                        n_s = st.selectbox("Status", ["Not Started", "In Progress", "Done"], index=["Not Started", "In Progress", "Done"].index(row['Status']), key=f"s_{row['Task_ID']}")
                        if n_s != row['Status']:
                            idx = df_tasks.index[df_tasks['Task_ID'] == row['Task_ID']][0] + 2
                            update_cell("Tasks", idx, 5, n_s)
                            st.rerun()
                    with c2:
                        url = f"https://kohanicrm.streamlit.app/?upload_token={row['Upload_Token']}"
                        st.text_input("Upload Link", url)

    # --- ADMIN ---
    elif nav == "🔒 Admin":
        st.title("Admin")
        t1, t2, t3 = st.tabs(["Migration", "Settings", "Automation"])
        
        with t1: render_migration_tool(data_dict)
        with t2: st.dataframe(data_dict.get('Services_Settings'))
        with t3:
            st.write("Force the system to check for new tasks based on assigned services.")
            if st.button("⚡ Force Run Automation Now"):
                run_daily_automation(data_dict, force=True)
