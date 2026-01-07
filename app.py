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
from streamlit_quill import st_quill # NEW RICH TEXT EDITOR

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

def send_email_as_user(to_email, subject, body_html):
    try:
        service = get_gmail_service()
        message = MIMEMultipart('alternative')
        
        sender_header = f"{st.session_state.user_name} <{st.session_state.user_email}>"
        message['to'] = to_email
        message['from'] = sender_header 
        message['cc'] = ADMIN_EMAIL 
        message['subject'] = subject
        
        # Strip HTML tags for plain text fallback
        plain_text = body_html.replace("<br>", "\n").replace("<div>", "").replace("</div>", "")
        
        part1 = MIMEText(plain_text, 'plain')
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
# 3. DATABASE FUNCTIONS
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
            # Ensure 'Title' and other columns exist
            for col in ['Status', 'Outcome', 'Internal_Flag', 'Notes', 'Last_Agent', 'Last_Updated', 'Title']:
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
        get_data.clear()
    except Exception as e:
        st.error(f"Save Error: {e}")

def clean_text(text):
    if not text: return ""
    return str(text).title().strip()

# ==========================================
# 4. GAMIFICATION & LOGIC
# ==========================================
def render_gamification(df):
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if 'Last_Updated' in df.columns:
        daily_df = df[df['Last_Updated'].str.contains(today_str, na=False)]
    else:
        daily_df = pd.DataFrame()
        
    my_calls = len(daily_df[daily_df['Last_Agent'] == st.session_state.user_email])
    
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        st.metric("📞 My Calls Today", my_calls)
    with c2:
        st.metric("🌍 Team Total", len(daily_df))
    with c3:
        if not daily_df.empty:
            leaders = daily_df['Last_Agent'].value_counts().reset_index()
            leaders.columns = ['Agent', 'Calls']
            st.dataframe(leaders, hide_index=True, use_container_width=True, height=100)

def generate_greeting(style, title, first, last):
    """Smart Greeting Generator"""
    first = clean_text(first)
    last = clean_text(last)
    title = clean_text(title)
    
    if style == "Casual (Hi Name)":
        return f"Hi {first},"
    elif style == "Formal (Dear Mr. Last)":
        # Fallback if Title missing
        t = title if title else ""
        return f"Dear {t} {last},"
    elif style == "Formal (Dear First Last)":
        return f"Dear {first} {last},"
    else:
        return "Hi,"

# ==========================================
# 5. SHARED COMPONENTS (CARD + EMAIL)
# ==========================================
def render_client_card_editor(df, templates, client_id):
    idx = df.index[df['ID'] == client_id][0]
    client = df.loc[idx]
    
    with st.container(border=True):
        c_h1, c_h2 = st.columns([3,1])
        c_h1.title(clean_text(client['Name']))
        c_h2.metric("Status", client['Status'])
        
        # --- EDIT FORM (With Title) ---
        with st.expander("📝 Edit Details (Title, Name, Phone)", expanded=True):
            # Row 1: Title & Names
            c_t, c_f, c_l = st.columns([1, 2, 2])
            new_title = c_t.selectbox("Title", ["", "Mr.", "Ms.", "Mrs.", "Dr."], 
                                      index=["", "Mr.", "Ms.", "Mrs.", "Dr."].index(client.get('Title')) if client.get('Title') in ["", "Mr.", "Ms.", "Mrs.", "Dr."] else 0)
            new_tp_first = c_f.text_input("First Name", clean_text(client.get('Taxpayer First Name')))
            new_tp_last = c_l.text_input("Last Name", clean_text(client.get('Taxpayer last name')))
            
            # Row 2: Contact
            c_ph, c_em = st.columns(2)
            new_phone = c_ph.text_input("Phone", client.get('Home Telephone'))
            new_email = c_em.text_input("Email", client.get('Taxpayer E-mail Address'))

        # --- OUTCOME & NOTES ---
        c1, c2 = st.columns(2)
        res = c1.selectbox("Call Result", ["Left Message", "Talked", "Wrong Number"], index=0 if client['Status'] not in ["Left Message", "Talked", "Wrong Number"] else ["Left Message", "Talked", "Wrong Number"].index(client['Status']))
        dec = c2.selectbox("Decision", ["Pending", "Yes", "No", "Maybe"], index=["Pending", "Yes", "No", "Maybe"].index(client['Outcome']) if client['Outcome'] in ["Pending", "Yes", "No", "Maybe"] else 0)
        
        st.markdown("### Notes")
        st.text_area("History", str(client.get('Notes', '')), disabled=True, height=80)
        new_note = st.text_area("Add Call Note")

        # --- EMAIL SECTION (RICH TEXT) ---
        st.markdown("---")
        st.subheader("📧 Send Follow-up")
        send_email = st.checkbox(f"Compose Email to {new_email}")
        
        final_html_body = ""
        subj = ""
        
        if send_email:
            # 1. Greeting Style
            col_g1, col_g2 = st.columns([1, 3])
            greeting_style = col_g1.selectbox("Greeting", ["Casual (Hi Name)", "Formal (Dear Mr. Last)", "Formal (Dear First Last)"])
            
            # Generate Greeting
            greeting_text = generate_greeting(greeting_style, new_title, new_tp_first, new_tp_last)
            
            # 2. Template
            if not templates.empty:
                t_opts = templates['Type'].unique()
                tmplt = col_g2.selectbox("Load Template", t_opts)
                
                t_row = templates[templates['Type'] == tmplt].iloc[0]
                subj = t_row['Subject']
                raw_body = t_row['Body']
                
                # Pre-fill Rich Text Editor
                # Combine Greeting + Body
                start_content = f"<p>{greeting_text}</p><p>{raw_body}</p>"
                
                col_sub = st.columns([1])[0]
                final_subj = col_sub.text_input("Subject", value=subj)
                
                st.caption("✍️ Edit Message (Rich Text)")
                # QUILL EDITOR
                content = st_quill(value=start_content, html=True, key=f"quill_{client_id}")
                
                if content:
                    sig = get_user_signature()
                    final_html_body = f"{content}<br><br>{sig}"
            else:
                st.warning("No templates.")

        # --- SAVE ACTIONS ---
        col_b1, col_b2 = st.columns([1,4])
        
        if col_b1.button("⬅️ Back"):
            st.session_state.current_id = None
            st.session_state.admin_current_id = None
            st.rerun()

        if col_b2.button("💾 SAVE & SEND", type="primary", use_container_width=True):
            # Update DB Vars
            df.at[idx, 'Title'] = new_title
            df.at[idx, 'Taxpayer First Name'] = new_tp_first
            df.at[idx, 'Taxpayer last name'] = new_tp_last
            df.at[idx, 'Home Telephone'] = new_phone
            df.at[idx, 'Taxpayer E-mail Address'] = new_email
            df.at[idx, 'Status'] = res
            df.at[idx, 'Outcome'] = dec
            df.at[idx, 'Last_Agent'] = st.session_state.user_email
            df.at[idx, 'Last_Updated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            
            if new_note:
                df.at[idx, 'Notes'] = str(client.get('Notes', '')) + f"\n[{st.session_state.user_email}]: {new_note}"

            # Send Email
            if send_email and new_email and final_html_body:
                if send_email_as_user(new_email, final_subj, final_html_body):
                    st.toast("Email Sent!")

            with st.spinner("Saving..."):
                update_data(df, "Clients")
            
            if dec == "Yes": st.balloons()
            st.success("Saved!")
            time.sleep(1)
            st.session_state.current_id = None
            st.session_state.admin_current_id = None
            st.rerun()

# ==========================================
# 6. VIEW: TEMPLATE MANAGER (RICH TEXT)
# ==========================================
def render_template_manager():
    st.subheader("📝 Template Manager (Rich Text)")
    
    df_temp = get_data("Templates")
    
    # Selector
    c1, c2 = st.columns([1, 1])
    
    # List existing
    t_list = df_temp['Type'].tolist() if not df_temp.empty else []
    t_list.insert(0, "➕ Create New Template")
    
    selected = c1.selectbox("Select Template to Edit", t_list)
    
    # State Vars for Edit
    if selected == "➕ Create New Template":
        curr_type = ""
        curr_subj = ""
        curr_body = "Write your email here..."
    else:
        row = df_temp[df_temp['Type'] == selected].iloc[0]
        curr_type = row['Type']
        curr_subj = row['Subject']
        curr_body = row['Body']

    # Edit Form
    with st.form("temp_edit"):
        new_type = st.text_input("Template Name (Internal)", value=curr_type)
        new_subj = st.text_input("Email Subject", value=curr_subj)
        
        st.write("**Email Body (Rich Text):**")
        # QUILL EDITOR FOR TEMPLATES
        new_body = st_quill(value=curr_body, html=True, key="temp_quill")
        
        if st.form_submit_button("💾 Save Template"):
            if selected == "➕ Create New Template":
                new_row = pd.DataFrame([{"Type": new_type, "Subject": new_subj, "Body": new_body}])
                df_temp = pd.concat([df_temp, new_row], ignore_index=True)
            else:
                idx = df_temp.index[df_temp['Type'] == selected][0]
                df_temp.at[idx, 'Type'] = new_type
                df_temp.at[idx, 'Subject'] = new_subj
                df_temp.at[idx, 'Body'] = new_body
            
            update_data(df_temp, "Templates")
            st.success("Saved!")
            time.sleep(1)
            st.rerun()

    # Delete Option
    if selected != "➕ Create New Template":
        if st.button("🗑️ Delete Template"):
            df_temp = df_temp[df_temp['Type'] != selected]
            update_data(df_temp, "Templates")
            st.rerun()

# ==========================================
# 7. ROUTING VIEWS
# ==========================================
def render_team_view(df, templates, user_email):
    if 'current_id' not in st.session_state: st.session_state.current_id = None
    
    if st.session_state.current_id is None:
        st.subheader("👋 Work Lobby")
        c1, c2 = st.columns(2)
        
        with c1:
            with st.container(border=True):
                st.write("### 📞 Call Queue")
                q = df[df['Status'] == 'New']
                st.metric("Clients", len(q))
                if not q.empty and st.button("🎲 START RANDOM CALL", type="primary", use_container_width=True):
                    st.session_state.current_id = q.sample(1).iloc[0]['ID']
                    st.rerun()
                    
        with c2:
            with st.container(border=True):
                st.write("### 🔎 Find Client")
                s = st.text_input("Search")
                if s:
                    res = df[df['Name'].astype(str).str.contains(s, case=False, na=False)]
                    if not res.empty:
                        for i, r in res.iterrows():
                            if st.button(f"LOAD: {r['Name']}", key=r['ID']):
                                st.session_state.current_id = r['ID']
                                st.rerun()
    else:
        render_client_card_editor(df, templates, st.session_state.current_id)

def render_admin_view(df, templates, user_email):
    st.title("🔒 Admin Dashboard")
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total", len(df))
    c2.metric("Calls", len(df[df['Status'] != 'New']))
    c3.metric("Pending", len(df[df['Outcome'].isin(['Pending', 'Maybe'])]))
    c4.metric("Success", len(df[df['Outcome'] == 'Yes']))
    
    t1, t2, t3, t4 = st.tabs(["📊 Activity", "📥 Inbox", "🔍 Database", "📝 Templates"])
    
    with t1:
        st.dataframe(df[df['Status'] != 'New'].sort_values('Last_Updated', ascending=False)[['Name', 'Status', 'Outcome', 'Last_Agent']])
    
    with t2:
        targets = df[(df['Outcome'] == 'Yes') & (df['Status'] != 'Manager Emailed')]
        if targets.empty:
            st.success("Inbox Zero")
        else:
            st.info("Select to Email")
            evt = st.dataframe(targets[['Name', 'Home Telephone', 'Notes']], on_select="rerun", selection_mode="single-row")
            if len(evt.selection.rows) > 0:
                cid = targets.iloc[evt.selection.rows[0]]['ID']
                render_client_card_editor(df, templates, cid)

    with t3: # Admin DB Search
        s = st.text_input("Admin Search")
        if s:
            res = df[df['Name'].astype(str).str.contains(s, case=False, na=False)]
            for i, r in res.iterrows():
                if st.button(f"EDIT: {r['Name']}", key=f"a_{r['ID']}"):
                    st.session_state.admin_current_id = r['ID']
                    st.rerun()
        if st.session_state.get('admin_current_id'):
            st.markdown("---")
            render_client_card_editor(df, templates, st.session_state.admin_current_id)

    with t4:
        render_template_manager()

# ==========================================
# 8. MAIN
# ==========================================
if not authenticate_user():
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        st.image("https://kohani.com/wp-content/uploads/2015/05/logo.png", width=200)
        st.title("Kohani CRM")
        flow = get_auth_flow()
        url, _ = flow.authorization_url(prompt='consent')
        st.link_button("🔵 Sign in with Google", url, type="primary")
else:
    user = st.session_state.user_email
    role = "Admin" if ("ali" in user or "admin" in user) else "Staff"
    
    with st.sidebar:
        st.write(f"👤 **{st.session_state.user_name}**")
        if st.button("🔄 Refresh"): get_data.clear(); st.rerun()
        if st.button("Logout"): del st.session_state.creds; st.rerun()
        
    df = get_data("Clients")
    temps = get_data("Templates")
    render_gamification(df)
    st.markdown("---")
    
    if role == "Admin": render_admin_view(df, temps, user)
    else: render_team_view(df, temps, user)
