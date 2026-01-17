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
    .status-New { background-color: #007bff; }
    .status-Pending { background-color: #ffc107; color: black; }
    .status-Done { background-color: #28a745; }
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
# A. SERVICE ACCOUNT (For DB & Drive Uploads - No User Interaction)
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

# B. USER OAUTH (For Gmail - Requires Login)
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
                # If tab missing, create empty DF
                data[t] = pd.DataFrame()
        return data, sh
    except Exception as e:
        st.error(f"DB Load Error: {e}")
        return {}, None

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
# 3. GOOGLE DRIVE & PUBLIC UPLOAD
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
        
        # Convert Streamlit UploadedFile to BytesIO
        fh = io.BytesIO(uploaded_file.getvalue())
        media = MediaIoBaseUpload(fh, mimetype=uploaded_file.type, resumable=True)
        
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')
    except Exception as e:
        return None

def render_public_upload_portal(token):
    """RENDERED FOR CLIENTS (NO LOGIN)"""
    st.markdown("## 📤 Secure Document Upload")
    
    # Check Token
    data, _ = get_all_data()
    df_tasks = data.get('Tasks', pd.DataFrame())
    
    if df_tasks.empty or 'Upload_Token' not in df_tasks.columns:
        st.error("System configuration error. Please contact support.")
        return

    task = df_tasks[df_tasks['Upload_Token'] == token]
    
    if task.empty:
        st.error("⚠️ Invalid or expired link.")
        return
        
    task_row = task.iloc[0]
    entity_id = task_row['Entity_ID']
    service_name = task_row['Service_Name']
    
    # Get Entity Name and Drive Folder
    df_ent = data.get('Entities')
    entity = df_ent[df_ent['ID'] == entity_id].iloc[0]
    ent_name = entity['Name']
    drive_id = entity.get('Drive_Folder_ID')
    
    st.success(f"Upload documents for: **{ent_name}** ({service_name})")
    
    if not drive_id:
        st.warning("Secure folder not set up for this client. Please contact the office.")
        return

    files = st.file_uploader("Drag and drop files here", accept_multiple_files=True)
    
    if st.button("🚀 Upload Files", type="primary"):
        if not files:
            st.error("Please select a file.")
        else:
            success_count = 0
            with st.status("Uploading..."):
                for f in files:
                    st.write(f"Uploading {f.name}...")
                    fid = upload_file_to_drive(f, drive_id)
                    if fid:
                        success_count += 1
            
            if success_count == len(files):
                st.balloons()
                st.success("✅ All files uploaded successfully! You may close this tab.")
                # Mark Task as 'Docs Uploaded' in DB? (Optional)
            else:
                st.warning(f"Uploaded {success_count} / {len(files)} files.")

# ==========================================
# 4. AUTOMATION & LOGIC (WAKE-ON-LOGIN)
# ==========================================
def generate_id(prefix):
    return f"{prefix}-{str(uuid.uuid4())[:8]}"

def run_daily_automation(data):
    """Runs once per day per 'wake up'."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    df_logs = data.get('App_Logs', pd.DataFrame())
    
    # Check if run today
    if not df_logs.empty:
        if df_logs['Timestamp'].str.contains(today).any():
            return # Already ran
            
    # --- LOGIC: GENERATE TASKS ---
    df_services = data.get('Services_Settings', pd.DataFrame())
    df_assigned = data.get('Services_Assigned', pd.DataFrame())
    df_tasks = data.get('Tasks', pd.DataFrame())
    
    new_tasks = []
    
    if not df_services.empty and not df_assigned.empty:
        for _, assign in df_assigned.iterrows():
            s_name = assign['Service_Name']
            e_id = assign['Entity_ID']
            
            # Find rules
            rule = df_services[df_services['Service_Name'] == s_name]
            if not rule.empty:
                freq = rule.iloc[0]['Frequency']
                # Simplistic Due Date Logic (Can be expanded)
                now = datetime.datetime.now()
                due_date_str = ""
                
                if freq == "Monthly":
                    # Due 15th of next month
                    next_month = now.month + 1 if now.month < 12 else 1
                    year = now.year if now.month < 12 else now.year + 1
                    due_date_str = f"{year}-{next_month:02d}-15"
                elif freq == "Quarterly":
                    # Q1: Apr 30, Q2: Jul 31, Q3: Oct 31, Q4: Jan 31
                    pass # (Implement logic as needed, keeping simple for demo)
                    due_date_str = (now + datetime.timedelta(days=30)).strftime("%Y-%m-%d") # Placeholder

                if due_date_str:
                    # Check duplicate
                    duplicate = False
                    if not df_tasks.empty:
                        # Check if task exists for this entity + service + due date
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
    else:
        append_to_sheet("App_Logs", [datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "Daily Check", "No new tasks"])


# ==========================================
# 5. UI: MIGRATION TOOL
# ==========================================
def render_migration_tool(data):
    st.subheader("🛠️ Data Migration Tool")
    st.warning("This tool moves data from 'Clients' to the new 'Entities' and 'Contacts' tabs.")
    
    df_old = data.get('Clients')
    
    if df_old.empty:
        st.info("No legacy data found.")
        return

    st.write(f"Found {len(df_old)} rows in Legacy Clients.")
    
    if st.button("🚀 RUN MIGRATION (With Call History)"):
        progress = st.progress(0)
        count = 0
        for i, row in df_old.iterrows():
            # 1. Create Entity
            ent_name = row.get('Name') or "Unknown"
            ent_id = generate_id("E")
            # Save Entity
            append_to_sheet("Entities", [ent_id, ent_name, "Unknown", "", "", "", ""])
            
            # Capture CRM Status
            status = row.get('Status', 'New')
            outcome = row.get('Outcome', '')
            last_agent = row.get('Last_Agent', '')
            last_updated = row.get('Last_Updated', '')
            
            # 2. Create Taxpayer Contact (Primary)
            tp_first = row.get('Taxpayer First Name')
            tp_email = row.get('Taxpayer E-mail Address')
            if tp_first or ent_name: # Ensure we create a contact even if just company name
                c_id = generate_id("C")
                # Add Status columns to Contact
                append_to_sheet("Contacts", [
                    c_id, 
                    tp_first if tp_first else ent_name, 
                    row.get('Taxpayer last name', ''), 
                    tp_email, 
                    row.get('Home Telephone', ''), 
                    "Taxpayer", 
                    "", # Notes (initial blank)
                    status, 
                    outcome, 
                    last_agent, 
                    last_updated
                ])
                append_to_sheet("Relationships", [ent_id, c_id, "Owner", "100%"])
            
            # 3. Create Spouse Contact (Secondary - usually Reset status or Same?)
            sp_first = row.get('Spouse First Name')
            if sp_first:
                c_id_sp = generate_id("C")
                append_to_sheet("Contacts", [
                    c_id_sp, 
                    sp_first, 
                    row.get('Spouse last name', ''), 
                    row.get('Spouse E-mail Address', ''), 
                    "", 
                    "Spouse", 
                    "",
                    "New", "", "", "" # Spouse starts fresh or linked? Keeping fresh for now.
                ])
                append_to_sheet("Relationships", [ent_id, c_id_sp, "Spouse", ""])
            
            count += 1
            progress.progress(count / len(df_old))
            
        st.success("Migration Complete! CRM History Preserved.")
        st.rerun()

# ==========================================
# 6. UI: MAIN APP COMPONENTS
# ==========================================
def get_gmail_service():
    if "creds" not in st.session_state: return None
    return build('gmail', 'v1', credentials=st.session_state.creds)

def send_email_as_user(to_email, subject, body_text, body_html):
    try:
        service = get_gmail_service()
        message = MIMEMultipart('alternative')
        sender = f"{st.session_state.user_name} <{st.session_state.user_email}>"
        message['to'] = to_email
        message['from'] = sender
        if st.session_state.user_email.lower() != ADMIN_EMAIL.lower():
            message['cc'] = ADMIN_EMAIL
        message['subject'] = subject
        part1 = MIMEText(body_text, 'plain')
        part2 = MIMEText(body_html, 'html')
        message.attach(part1)
        message.attach(part2)
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        service.users().messages().send(userId='me', body={'raw': raw}).execute()
        return True
    except Exception as e:
        st.error(f"Gmail Error: {e}")
        return False

# ==========================================
# 7. MAIN ROUTER
# ==========================================

# CHECK FOR PUBLIC UPLOAD LINK
if "upload_token" in st.query_params:
    render_public_upload_portal(st.query_params["upload_token"])
    st.stop() # STOP EXECUTION HERE FOR CLIENTS

# ADMIN LOGIN
if not authenticate_user():
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        st.image("https://kohani.com/wp-content/uploads/2015/05/logo.png", width=200)
        st.title("Kohani Practice Management")
        flow = get_auth_flow()
        auth_url, _ = flow.authorization_url(prompt='consent')
        st.link_button("🔵 Sign in with Google", auth_url, type="primary")
else:
    # --- APP WAKE UP ---
    data_dict, sh_obj = get_all_data()
    run_daily_automation(data_dict) # WAKE ON LOGIN

    with st.sidebar:
        st.write(f"👤 **{st.session_state.user_name}**")
        st.markdown("---")
        nav = st.radio("Navigation", ["📊 Dashboard", "📞 Call Queue", "🏢 Entities", "👥 Contacts", "✅ Production (Tasks)", "🔒 Admin"])
        st.markdown("---")
        if st.button("Logout"):
            del st.session_state.creds
            st.rerun()

    # --- DASHBOARD ---
    if nav == "📊 Dashboard":
        st.title("Practice Dashboard")
        df_tasks = data_dict.get('Tasks', pd.DataFrame())
        df_ent = data_dict.get('Entities', pd.DataFrame())

    # --- CALL QUEUE (RESTORED) ---
    elif nav == "📞 Call Queue":
        st.title("📞 Call Queue & CRM")
        df_con = data_dict.get('Contacts')
        
        # Gamification / Stats
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        if 'Last_Updated' in df_con.columns:
            daily = df_con[df_con['Last_Updated'].str.contains(today_str, na=False)]
            my_calls = len(daily[daily['Last_Agent'] == st.session_state.user_email])
            st.metric("🔥 My Calls Today", my_calls)
        
        # Logic
        if "call_session_id" not in st.session_state: st.session_state.call_session_id = None
        
        if st.session_state.call_session_id is None:
            # LOBBY VIEW
            c1, c2 = st.columns([1, 1])
            with c1:
                st.subheader("Action Required")
                # Filter for New or Follow Up
                queue = df_con[df_con['Status'].isin(['New', 'Follow Up', 'Left Message'])]
                st.write(f"**{len(queue)}** contacts in queue.")
                
                if st.button("🎲 START NEXT CALL", type="primary", use_container_width=True):
                    if not queue.empty:
                        # Prioritize leads with phone numbers
                        has_phone = queue[queue['Phone'].str.len() > 5]
                        if not has_phone.empty:
                            selected = has_phone.sample(1).iloc[0]
                        else:
                            selected = queue.sample(1).iloc[0]
                        st.session_state.call_session_id = selected['ID']
                        st.rerun()
                    else:
                        st.success("Queue cleared! 🎉")
            
            with c2:
                st.subheader("Manual Search")
                search_q = st.text_input("Search Contact Name/Phone")
                if search_q:
                    res = df_con[df_con['First Name'].astype(str).str.contains(search_q, case=False) | 
                                 df_con['Phone'].astype(str).str.contains(search_q)]
                    if not res.empty:
                        for _, r in res.iterrows():
                            if st.button(f"Load: {r['First Name']} {r['Last Name']}", key=f"btn_{r['ID']}"):
                                st.session_state.call_session_id = r['ID']
                                st.rerun()

        else:
            # CARD VIEW
            cid = st.session_state.call_session_id
            contact = df_con[df_con['ID'] == cid].iloc[0]
            
            with st.container(border=True):
                c_head, c_btn = st.columns([3, 1])
                c_head.title(f"{contact['First Name']} {contact['Last Name']}")
                if c_btn.button("❌ Exit Call"):
                    st.session_state.call_session_id = None
                    st.rerun()
                
                c1, c2 = st.columns(2)
                new_phone = c1.text_input("Phone", contact['Phone'])
                new_email = c2.text_input("Email", contact['Email'])
                
                st.info(f"Current Status: **{contact.get('Status', 'New')}**")
                
                # Notes Section
                notes_hist = str(contact.get('Notes', ''))
                st.text_area("History", notes_hist, height=150, disabled=True)
                new_note = st.text_area("New Note / Call Log")
                
                # Disposition
                col_res1, col_res2 = st.columns(2)
                res_status = col_res1.selectbox("New Status", ["New", "Left Message", "Talked", "Wrong Number", "Not Interested", "Sold/Won"])
                res_outcome = col_res2.selectbox("Outcome", ["Pending", "Yes", "No", "Maybe"])
                
                if st.button("💾 SAVE & FINISH", type="primary", use_container_width=True):
                    # Find Row Index
                    idx = df_con.index[df_con['ID'] == cid][0] + 2 # +2 for header and 1-based
                    
                    # Update Cells (Example columns - adjust indexes if your sheet varies)
                    # Assuming: 5=Phone, 4=Email, 7=Notes, 8=Status, 9=Outcome, 10=Agent, 11=Time
                    update_cell("Contacts", idx, 5, new_phone)
                    update_cell("Contacts", idx, 4, new_email)
                    update_cell("Contacts", idx, 8, res_status)
                    update_cell("Contacts", idx, 9, res_outcome)
                    update_cell("Contacts", idx, 10, st.session_state.user_email)
                    update_cell("Contacts", idx, 11, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
                    
                    if new_note:
                        ts = datetime.datetime.now().strftime("%Y-%m-%d")
                        append_note = f"\n[{ts} {st.session_state.user_name}]: {new_note}"
                        final_note = notes_hist + append_note
                        update_cell("Contacts", idx, 7, final_note)
                        
                    st.success("Saved!")
                    time.sleep(1)
                    st.session_state.call_session_id = None
                    st.rerun()
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Active Entities", len(df_ent))
        pending = len(df_tasks[df_tasks['Status'] != 'Done']) if not df_tasks.empty else 0
        c2.metric("Pending Tasks", pending)
        c3.metric("System Status", "Online 🟢")

    # --- ENTITIES MANAGER ---
    elif nav == "🏢 Entities":
        st.title("Entity Manager")
        df_ent = data_dict.get('Entities')
        df_rel = data_dict.get('Relationships')
        df_con = data_dict.get('Contacts')
        
        col_list, col_detail = st.columns([1, 2])
        
        selected_ent_id = None
        with col_list:
            search = st.text_input("Search Entity")
            if not df_ent.empty:
                if search:
                    mask = df_ent['Name'].str.contains(search, case=False, na=False)
                    disp = df_ent[mask]
                else:
                    disp = df_ent
                
                selected_idx = st.dataframe(disp[['Name', 'Type']], on_select="rerun", selection_mode="single-row")
                if len(selected_idx.selection.rows) > 0:
                    row_i = selected_idx.selection.rows[0]
                    selected_ent_id = disp.iloc[row_i]['ID']

            if st.button("➕ New Entity"):
                st.session_state.new_entity_mode = True

        with col_detail:
            if st.session_state.get('new_entity_mode'):
                st.subheader("Create New Entity")
                n_name = st.text_input("Entity Name")
                n_type = st.selectbox("Type", ["LLC", "S-Corp", "C-Corp", "Individual", "Non-Profit"])
                if st.button("Save Entity"):
                    nid = generate_id("E")
                    append_to_sheet("Entities", [nid, n_name, n_type, "", "", "", ""])
                    st.session_state.new_entity_mode = False
                    st.rerun()
            
            elif selected_ent_id:
                ent_row = df_ent[df_ent['ID'] == selected_ent_id].iloc[0]
                st.header(ent_row['Name'])
                
                # DETAILS TAB
                t1, t2, t3 = st.tabs(["Details & Drive", "People (Owners)", "Services"])
                
                with t1:
                    st.write(f"**Type:** {ent_row['Type']}")
                    st.write(f"**FEIN:** {ent_row.get('FEIN')}")
                    
                    # DRIVE FOLDER LOGIC
                    curr_drive = ent_row.get('Drive_Folder_ID')
                    if not curr_drive:
                        st.warning("⚠️ No Drive Folder linked.")
                        if st.button("📂 Create Drive Folder"):
                            fid = create_drive_folder(f"{ent_row['Name']} - {selected_ent_id}")
                            # Update DB
                            # Find row index (1-based + header)
                            raw_idx = df_ent.index[df_ent['ID'] == selected_ent_id][0] + 2
                            # Assuming Drive ID is Col 7
                            update_cell("Entities", raw_idx, 7, fid)
                            st.success("Folder Created!")
                            st.rerun()
                    else:
                        st.success(f"📂 Drive Folder Linked: {curr_drive}")
                        # Link to open it?
                        st.markdown(f"[Open in Google Drive](https://drive.google.com/drive/u/0/folders/{curr_drive})")

                with t2:
                    st.subheader("Connected People")
                    # Show relationships
                    if not df_rel.empty:
                        rels = df_rel[df_rel['Entity_ID'] == selected_ent_id]
                        if not rels.empty:
                            for _, r in rels.iterrows():
                                c_info = df_con[df_con['ID'] == r['Contact_ID']]
                                if not c_info.empty:
                                    c_name = f"{c_info.iloc[0]['First Name']} {c_info.iloc[0]['Last Name']}"
                                    st.write(f"👤 **{c_name}** - {r['Role']}")

                with t3:
                    st.subheader("Assigned Services")
                    df_assign = data_dict.get('Services_Assigned')
                    if not df_assign.empty:
                        my_servs = df_assign[df_assign['Entity_ID'] == selected_ent_id]
                        st.dataframe(my_servs[['Service_Name', 'Start_Date']])
                    
                    df_set = data_dict.get('Services_Settings')
                    if not df_set.empty:
                        new_s = st.selectbox("Add Service", df_set['Service_Name'].unique())
                        if st.button("Add"):
                            append_to_sheet("Services_Assigned", [selected_ent_id, new_s, datetime.datetime.now().strftime("%Y-%m-%d"), ""])
                            st.rerun()

    # --- PRODUCTION / TASKS ---
    elif nav == "✅ Production (Tasks)":
        st.title("Production Calendar")
        df_tasks = data_dict.get('Tasks')
        df_ent = data_dict.get('Entities')
        
        if df_tasks.empty:
            st.info("No tasks scheduled.")
        else:
            # Merge with Entity Names for display
            df_full = df_tasks.merge(df_ent[['ID', 'Name']], left_on='Entity_ID', right_on='ID', how='left')
            
            # Filters
            f_status = st.multiselect("Filter Status", df_tasks['Status'].unique(), default=["Not Started", "In Progress"])
            
            view_df = df_full[df_full['Status'].isin(f_status)] if f_status else df_full
            
            for i, row in view_df.iterrows():
                with st.expander(f"📅 {row['Due_Date']} | {row['Name']} | {row['Service_Name']}"):
                    c1, c2, c3 = st.columns(3)
                    
                    with c1:
                        new_stat = st.selectbox("Status", ["Not Started", "In Progress", "Done"], index=["Not Started", "In Progress", "Done"].index(row['Status']), key=f"stat_{row['Task_ID']}")
                        if new_stat != row['Status']:
                            # Update DB
                            # Look up row index in original df
                            idx = df_tasks.index[df_tasks['Task_ID'] == row['Task_ID']][0] + 2
                            update_cell("Tasks", idx, 5, new_stat) # Col 5 is Status
                            st.toast("Status Updated")
                            time.sleep(1)
                            st.rerun()
                    
                    with c2:
                        st.write(f"**Docs Uploaded:** {row.get('Docs_Uploaded', 'No')}")
                        # Generate Link
                        base_url = "https://kohanicrm.streamlit.app" # CHANGE THIS TO YOUR REAL URL IF DIFFERENT
                        link = f"{base_url}/?upload_token={row['Upload_Token']}"
                        st.text_input("🔗 Client Upload Link", link)

                    with c3:
                        # Email Client Logic
                        st.write("**Quick Actions**")
                        if st.button("✉️ Send Reminder", key=f"email_{row['Task_ID']}"):
                            # Look up contact email
                            # (Simplified: Just grabbing first owner)
                            st.toast("Email feature linked to Contacts (Coming in v2)")

    # --- ADMIN ---
    elif nav == "🔒 Admin":
        st.title("Admin Panel")
        
        tab_mig, tab_set, tab_logs = st.tabs(["Data Migration", "Service Settings", "App Logs"])
        
        with tab_mig:
            render_migration_tool(data_dict)
            
        with tab_set:
            st.subheader("Define Services & Frequencies")
            df_set = data_dict.get('Services_Settings')
            st.dataframe(df_set)
            st.info("Edit these directly in Google Sheets tab 'Services_Settings'")

        with tab_logs:
            st.dataframe(data_dict.get('App_Logs'))
