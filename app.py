import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
import time
import socket
import plotly.express as px

# ==========================================
# 0. CONFIG & NETWORK SAFETY
# ==========================================
socket.setdefaulttimeout(30)
st.set_page_config(page_title="Kohani CRM", page_icon="📊", layout="wide")

st.markdown("""
    <style>
    #MainMenu {display: none;}
    header {visibility: hidden;}
    div.stButton > button:first-child {
        background-color: #004B87; color: white; border-radius: 8px; font-weight: bold;
    }
    .stDataFrame { border: 1px solid #ddd; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

# Admin Email for CC
ADMIN_EMAIL = "ali@kohani.com"

SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.settings.basic',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile'
]

# ==========================================
# 1. AUTHENTICATION
# ==========================================
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
        scopes=SCOPES,
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
# 2. GMAIL FUNCTIONS
# ==========================================
def get_gmail_service():
    if "creds" not in st.session_state: return None
    return build('gmail', 'v1', credentials=st.session_state.creds)

def get_user_signature():
    try:
        service = get_gmail_service()
        sendas_list = service.users().settings().sendAs().list(userId='me').execute()
        for alias in sendas_list.get('sendAs', []):
            if alias.get('isPrimary') or alias.get('sendAsEmail') == st.session_state.user_email:
                return alias.get('signature', '') 
        return ""
    except:
        return ""

def send_email_as_user(to_email, subject, body_text, body_html):
    try:
        service = get_gmail_service()
        message = MIMEMultipart('alternative')
        
        sender_header = f"{st.session_state.user_name} <{st.session_state.user_email}>"
        message['to'] = to_email
        message['from'] = sender_header 
        message['cc'] = ADMIN_EMAIL 
        message['subject'] = subject
        
        part1 = MIMEText(body_text, 'plain')
        part2 = MIMEText(body_html, 'html')
        message.attach(part1)
        message.attach(part2)
        
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        body = {'raw': raw}
        
        service.users().messages().send(userId='me', body=body).execute()
        return True
    except Exception as e:
        st.error(f"Gmail Error: {e}")
        return False

# ==========================================
# 3. DATABASE FUNCTIONS (CACHED)
# ==========================================
def get_db_client():
    if "connections" not in st.secrets:
        st.error("❌ Secrets missing.")
        st.stop()
    secrets_dict = dict(st.secrets["connections"]["gsheets"])
    if "private_key" in secrets_dict:
        secrets_dict["private_key"] = secrets_dict["private_key"].replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(secrets_dict, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=600) 
def get_data(worksheet_name="Clients"):
    try:
        client = get_db_client()
        raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
        sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
        sh = client.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(worksheet_name)
        except:
            if worksheet_name == "Templates": return pd.DataFrame(columns=['Type', 'Subject', 'Body'])
            return pd.DataFrame()
            
        raw_data = ws.get_all_values()
        if not raw_data: return pd.DataFrame()
        
        headers = raw_data[0]
        rows = raw_data[1:]
        unique_headers = []
        seen = {}
        for h in headers:
            clean_h = str(h).strip()
            if clean_h in seen: seen[clean_h] += 1; unique_headers.append(f"{clean_h}_{seen[clean_h]}")
            else: seen[clean_h] = 0; unique_headers.append(clean_h)
            
        df = pd.DataFrame(rows, columns=unique_headers)
        
        if worksheet_name == "Clients":
            for col in ['Status', 'Outcome', 'Internal_Flag', 'Notes', 'Last_Agent', 'Last_Updated']:
                if col not in df.columns: df[col] = ""
            df['Status'] = df['Status'].replace("", "New")
        return df
    except Exception as e:
        return pd.DataFrame()

def update_data(df, worksheet_name="Clients"):
    try:
        client = get_db_client()
        raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
        sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
        sh = client.open_by_key(sheet_id)
        ws = sh.worksheet(worksheet_name)
        ws.clear()
        ws.update([df.columns.values.tolist()] + df.values.tolist())
        # Clear cache to force refresh
        get_data.clear()
    except Exception as e:
        st.error(f"Save Error: {e}")

def clean_text(text):
    if not text: return ""
    return str(text).title().strip()

# ==========================================
# 4. GAMIFICATION & STATS
# ==========================================
def render_gamification(df):
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # Filter for Today's Activity
    if 'Last_Updated' in df.columns:
        daily_df = df[df['Last_Updated'].str.contains(today_str, na=False)]
    else:
        daily_df = pd.DataFrame()
        
    my_calls = len(daily_df[daily_df['Last_Agent'] == st.session_state.user_email])
    total_daily = len(daily_df)
    
    # 1. My Daily Progress
    target = 20 # Daily Goal
    progress = min(my_calls / target, 1.0)
    
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        st.metric("📞 My Calls Today", f"{my_calls} / {target}")
        st.progress(progress)
        if my_calls >= target: st.balloons()
        
    with c2:
        st.metric("🌍 Team Total Today", total_daily)
    
    with c3:
        # Leaderboard
        if not daily_df.empty:
            leaders = daily_df['Last_Agent'].value_counts().reset_index()
            leaders.columns = ['Agent', 'Calls']
            # Simple Badge Logic
            leaders['Rank'] = leaders['Calls'].apply(lambda x: "🔥" if x >= 15 else "⭐")
            st.dataframe(leaders[['Rank', 'Agent', 'Calls']], hide_index=True, use_container_width=True, height=120)
        else:
            st.caption("No calls yet today. Be the first!")

# ==========================================
# 5. SHARED COMPONENTS
# ==========================================
def render_client_card_editor(df, templates, client_id):
    """
    Shared Logic: Used by both Team and Admin to View/Edit/Email a client.
    """
    # Isolate Client
    idx = df.index[df['ID'] == client_id][0]
    client = df.loc[idx]
    
    with st.container(border=True):
        c_h1, c_h2 = st.columns([3,1])
        c_h1.title(clean_text(client['Name']))
        c_h2.metric("Status", client['Status'])
        
        # --- EDIT FORM ---
        with st.expander("📝 Edit Details", expanded=True):
            c1, c2 = st.columns(2)
            new_tp_first = c1.text_input("TP First Name", clean_text(client.get('Taxpayer First Name')))
            new_sp_first = c2.text_input("SP First Name", clean_text(client.get('Spouse First Name')))
            c3, c4 = st.columns(2)
            new_tp_last = c3.text_input("TP Last Name", clean_text(client.get('Taxpayer last name')))
            new_sp_last = c4.text_input("SP Last Name", clean_text(client.get('Spouse last name')))
            c5, c6 = st.columns(2)
            new_phone = c5.text_input("Phone", client.get('Home Telephone'))
            new_email = c6.text_input("Email", client.get('Taxpayer E-mail Address'))

        # --- NOTES ---
        st.markdown("### Notes")
        st.text_area("History", str(client.get('Notes', '')), disabled=True, height=100)
        new_note = st.text_area("Add Note")

        # --- OUTCOME ---
        c_out1, c_out2 = st.columns(2)
        res = c_out1.selectbox("Result", ["Left Message", "Talked", "Wrong Number"], index=0 if client['Status'] not in ["Left Message", "Talked", "Wrong Number"] else ["Left Message", "Talked", "Wrong Number"].index(client['Status']))
        dec = c_out2.selectbox("Decision", ["Pending", "Yes", "No", "Maybe"], index=["Pending", "Yes", "No", "Maybe"].index(client['Outcome']) if client['Outcome'] in ["Pending", "Yes", "No", "Maybe"] else 0)
        flag = st.checkbox("🚩 Internal Flag", value=(str(client.get('Internal_Flag')) == 'TRUE'))

        # --- EMAIL ---
        st.markdown("### Email")
        send_email = st.checkbox(f"Send to {new_email}")
        final_html = ""
        final_text = ""
        subj = ""
        
        if send_email:
            if not templates.empty:
                tmplt = st.selectbox("Template", templates['Type'].unique())
                if not templates[templates['Type'] == tmplt].empty:
                    raw_body = templates[templates['Type'] == tmplt]['Body'].values[0]
                    subj = templates[templates['Type'] == tmplt]['Subject'].values[0]
                    
                    sig = get_user_signature()
                    disp_name = new_tp_first if new_tp_first else "Client"
                    body_edit = st.text_area("Edit Message", value=raw_body.replace("{Name}", disp_name), height=150)
                    final_text = body_edit
                    final_html = f"{body_edit.replace(chr(10), '<br>')}<br><br>{sig}"
            else:
                st.warning("No templates.")

        if st.button("💾 SAVE & FINISH", type="primary"):
            # Update
            df.at[idx, 'Taxpayer First Name'] = new_tp_first
            df.at[idx, 'Spouse First Name'] = new_sp_first
            df.at[idx, 'Taxpayer last name'] = new_tp_last
            df.at[idx, 'Spouse last name'] = new_sp_last
            df.at[idx, 'Home Telephone'] = new_phone
            df.at[idx, 'Taxpayer E-mail Address'] = new_email
            df.at[idx, 'Status'] = res
            df.at[idx, 'Outcome'] = dec
            df.at[idx, 'Internal_Flag'] = "TRUE" if flag else "FALSE"
            df.at[idx, 'Last_Agent'] = st.session_state.user_email
            df.at[idx, 'Last_Updated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            
            if new_note:
                df.at[idx, 'Notes'] = str(client.get('Notes', '')) + f"\n[{st.session_state.user_email}]: {new_note}"

            if send_email and new_email:
                if send_email_as_user(new_email, subj, final_text, final_html):
                    st.toast(f"Email sent to {new_email}")

            with st.spinner("Saving..."):
                update_data(df, "Clients")
            
            if dec == "Yes": st.balloons()
            st.success("Saved!")
            time.sleep(1)
            st.session_state.current_id = None
            st.rerun()

# ==========================================
# 6. VIEW: TEMPLATE MANAGER (ADMIN ONLY)
# ==========================================
def render_template_manager():
    st.subheader("📝 Template Manager")
    
    # Cheat Sheet
    with st.expander("HTML Cheat Sheet (How to format emails)"):
        st.markdown("""
        - **Bold:** `<b>Text</b>` -> <b>Text</b>
        - **Italic:** `<i>Text</i>` -> <i>Text</i>
        - **Link:** `<a href="https://kohani.com">Link Name</a>` -> <a href="#">Link Name</a>
        - **Line Break:** Press Enter (automatically converts) or use `<br>`
        """)

    df_temp = get_data("Templates")
    
    # Editor
    edited_df = st.data_editor(df_temp, num_rows="dynamic", use_container_width=True)
    
    if st.button("💾 Save Templates"):
        update_data(edited_df, "Templates")
        st.success("Templates updated!")
        
    st.markdown("---")
    st.subheader("👁️ Live Preview")
    
    if not edited_df.empty:
        prev_temp = st.selectbox("Preview Template", edited_df['Type'].unique())
        row = edited_df[edited_df['Type'] == prev_temp].iloc[0]
        st.write(f"**Subject:** {row['Subject']}")
        
        # Render HTML
        dummy_body = row['Body'].replace("{Name}", "John Doe").replace("\n", "<br>")
        st.caption("How it will look to the client:")
        st.html(f"<div style='border:1px solid #ccc; padding:15px; border-radius:5px;'>{dummy_body}</div>")

# ==========================================
# 7. ROUTER & SIDEBAR
# ==========================================
if not authenticate_user():
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        st.image("https://kohani.com/wp-content/uploads/2015/05/logo.png", width=200)
        st.title("Kohani CRM Login")
        flow = get_auth_flow()
        auth_url, _ = flow.authorization_url(prompt='consent')
        st.link_button("🔵 Sign in with Google", auth_url, type="primary")
else:
    user_email = st.session_state.user_email
    role = "Admin" if ("ali" in user_email or "admin" in user_email) else "Staff"
    
    # Sidebar
    with st.sidebar:
        user_name = st.session_state.get('user_name', user_email)
        st.write(f"👤 **{user_name}**")
        st.caption(f"Role: {role}")
        
        if st.button("🔄 Refresh Data"):
            get_data.clear()
            st.rerun()
            
        if st.button("Logout"):
            del st.session_state.creds; del st.session_state.user_email; st.rerun()
            
    # Load Data
    df = get_data("Clients")
    templates = get_data("Templates")
    
    # Show Gamification Stats to everyone
    render_gamification(df)
    st.markdown("---")

    # Routing
    if role == "Admin":
        tabs = st.tabs(["📊 Dashboard", "📥 Inbox", "🔍 Database & Edit", "📝 Templates"])
        
        with tabs[0]: # Dashboard
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Clients", len(df))
            c2.metric("Calls Made", len(df[df['Status'] != 'New']))
            c3.metric("Pending", len(df[df['Outcome'].isin(['Pending', 'Maybe'])]))
            c4.metric("Success", len(df[df['Outcome'] == 'Yes']))
            
            st.subheader("All Activity Log")
            activity = df[df['Status'] != 'New'].sort_values(by='Last_Updated', ascending=False)
            st.dataframe(activity[['Name', 'Status', 'Outcome', 'Last_Updated', 'Last_Agent']])

        with tabs[1]: # Inbox
            st.subheader("Waiting for Manager Email (Yes)")
            targets = df[(df['Outcome'] == 'Yes') & (df['Status'] != 'Manager Emailed')]
            if targets.empty:
                st.success("Inbox Zero!")
            else:
                st.info("Select a row to email.")
                event = st.dataframe(targets[['Name', 'Home Telephone', 'Notes']], on_select="rerun", selection_mode="single-row", use_container_width=True)
                if len(event.selection.rows) > 0:
                    row_idx = event.selection.rows[0]
                    client_id = targets.iloc[row_idx]['ID']
                    render_client_card_editor(df, templates, client_id)

        with tabs[2]: # Database (Same as Team View)
            st.subheader("Full Database Search")
            search = st.text_input("Search Name/Phone/Email")
            if search:
                res = df[df['Name'].astype(str).str.contains(search, case=False, na=False)]
                if not res.empty:
                    res['Label'] = res['Name'] + " (" + res['Status'] + ")"
                    target = st.selectbox("Select Client", res['Label'])
                    t_id = res[res['Label'] == target]['ID'].values[0]
                    if st.button("Load Client Card"):
                        st.session_state.admin_current_id = t_id
                        st.rerun()
            
            if 'admin_current_id' in st.session_state:
                render_client_card_editor(df, templates, st.session_state.admin_current_id)

        with tabs[3]: # Templates
            render_template_manager()

    else: # STAFF VIEW
        t1, t2 = st.tabs(["📞 Call Queue", "🔎 Search"])
        
        with t1:
            if 'current_id' not in st.session_state: st.session_state.current_id = None
            if st.session_state.current_id is None:
                queue = df[df['Status'] == 'New']
                st.info(f"Queue: {len(queue)} clients")
                if not queue.empty and st.button("📞 START NEXT CALL", type="primary"):
                    st.session_state.current_id = queue.sample(1).iloc[0]['ID']
                    st.rerun()
            else:
                render_client_card_editor(df, templates, st.session_state.current_id)
                if st.button("Cancel / Next"):
                    st.session_state.current_id = None; st.rerun()

        with t2:
            search = st.text_input("Find Client")
            if search:
                res = df[df['Name'].astype(str).str.contains(search, case=False, na=False)]
                if not res.empty:
                    res['Label'] = res['Name'] + " (" + res['Status'] + ")"
                    target = st.selectbox("Select", res['Label'])
                    t_id = res[res['Label'] == target]['ID'].values[0]
                    if st.button("Load"):
                        st.session_state.current_id = t_id
                        st.rerun()
