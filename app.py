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
import re

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
    textarea { font-family: monospace; }
    /* Highlight for the reference match box */
    .reference-box {
        background-color: #e3f2fd;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #004B87;
        margin-bottom: 15px;
    }
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
        
        # Only CC if the sender is NOT the admin
        # using .lower() to ensure case-insensitive comparison
        if st.session_state.user_email.lower() != ADMIN_EMAIL.lower():
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
            # Return empty for Reference if not found
            if worksheet_name == "Reference": return pd.DataFrame()
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
            # Ensure standard columns exist
            required_cols = ['Status', 'Outcome', 'Internal_Flag', 'Notes', 'Last_Agent', 'Last_Updated', 'Gender', 'Spouse E-mail Address']
            for col in required_cols:
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
    target = 50 # Daily Goal
    progress = min(my_calls / target, 1.0)
    
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        st.metric("📞 My Calls Today", f"{my_calls} / {target}")
        st.progress(progress)
        
        if my_calls >= target:
            if "goal_celebrated" not in st.session_state:
                st.balloons()
                st.session_state.goal_celebrated = True
        
    with c2:
        st.metric("🌍 Team Total Today", total_daily)
    
    with c3:
        # Leaderboard
        if not daily_df.empty:
            leaders = daily_df['Last_Agent'].value_counts().reset_index()
            leaders.columns = ['Agent', 'Calls']
            leaders['Rank'] = leaders['Calls'].apply(lambda x: "🔥" if x >= 15 else "⭐")
            st.dataframe(leaders[['Rank', 'Agent', 'Calls']], hide_index=True, use_container_width=True, height=120)
        else:
            st.caption("No calls yet today. Be the first!")

# ==========================================
# 5. LOGIC HELPERS
# ==========================================
def generate_greeting(style, first_name, last_name, gender):
    first_name = clean_text(first_name)
    last_name = clean_text(last_name)
    
    if style == "Casual":
        return f"Hi {first_name},"
    else: 
        if not last_name: return f"Dear {first_name},"
        
        prefix = ""
        if gender == "Male": prefix = "Mr."
        elif gender == "Female": prefix = "Ms."
        else: return f"Dear {first_name} {last_name}," 
        
        return f"Dear {prefix} {last_name},"

# ==========================================
# 6. SHARED COMPONENTS
# ==========================================
def render_client_card_editor(df, df_ref, templates, client_id):
    """
    Shared Logic: Used by both Team and Admin to View/Edit/Email a client.
    Includes Smart Reference Sheet Cross-Check (Bidirectional Overlap).
    """
    # Isolate Client
    idx = df.index[df['ID'] == client_id][0]
    client = df.loc[idx]
    
    with st.container(border=True):
        # Header
        c_h1, c_h2 = st.columns([3,1])
        c_h1.title(clean_text(client['Name']))
        c_h2.metric("Status", client['Status'])
        
        # --- EDIT FORM ---
        with st.expander("📝 Edit Details", expanded=True):
            # Row 1: Taxpayer Info + Gender
            c1, c2, c3 = st.columns([2, 2, 1])
            new_tp_first = c1.text_input("TP First Name", clean_text(client.get('Taxpayer First Name')))
            new_tp_last = c2.text_input("TP Last Name", clean_text(client.get('Taxpayer last name')))
            
            current_gender = client.get('Gender', 'Unknown')
            if current_gender not in ["Male", "Female", "Unknown"]: current_gender = "Unknown"
            new_gender = c3.selectbox("Gender", ["Male", "Female", "Unknown"], index=["Male", "Female", "Unknown"].index(current_gender))

            # Row 2: Spouse Info
            c4, c5 = st.columns(2)
            new_sp_first = c4.text_input("SP First Name", clean_text(client.get('Spouse First Name')))
            new_sp_last = c5.text_input("SP Last Name", clean_text(client.get('Spouse last name')))
            
            # Row 3: Contact Info (Phone)
            st.write("**Contact Info**")
            
            # --- FEATURE 1: ROBUST FUZZY / TOKEN MATCHING ---
            current_phone_val = client.get('Home Telephone', '')
            found_ref_phone = None
            
            # Only search if current phone is missing or short
            if (not current_phone_val or len(str(current_phone_val)) < 5) and not df_ref.empty:
                
                # Dynamic Column Identification
                ref_cols = [str(c).lower().strip() for c in df_ref.columns]
                
                phone_keywords = ['phone', 'mobile', 'cell', 'tel', 'contact', 'number']
                name_keywords = ['name', 'client', 'customer', 'taxpayer', 'person']
                
                phone_col_idx = -1
                for i, col_name in enumerate(ref_cols):
                    if any(k in col_name for k in phone_keywords):
                        phone_col_idx = i; break
                        
                name_col_idx = -1
                for i, col_name in enumerate(ref_cols):
                    if any(k in col_name for k in name_keywords):
                        name_col_idx = i; break

                if phone_col_idx != -1 and name_col_idx != -1:
                    real_phone_col = df_ref.columns[phone_col_idx]
                    real_name_col = df_ref.columns[name_col_idx]
                    
                    # 1. Clean and Tokenize Client Name (Remove punctuation like ",")
                    client_name_str = str(client['Name']).lower()
                    client_tokens = set(re.findall(r'\w+', client_name_str))

                    if client_tokens:
                        # Define Token Matcher with Overlap Logic
                        def check_token_match(ref_val):
                            if not isinstance(ref_val, str): return False
                            ref_tokens = set(re.findall(r'\w+', ref_val.lower()))
                            
                            if not ref_tokens: return False
                            
                            # INTERSECTION: How many words are in common?
                            common = client_tokens.intersection(ref_tokens)
                            
                            # RULE A: Strong Match (2 or more words match)
                            # e.g. "Monica" + "Foroutan" matches -> True
                            if len(common) >= 2: return True
                            
                            # RULE B: Subset Match (Ref fits inside Client OR Client fits inside Ref)
                            # Handles "Monica V. Foroutan" vs "Monica Foroutan"
                            if ref_tokens.issubset(client_tokens): return True
                            if client_tokens.issubset(ref_tokens): return True
                            
                            return False

                        # Find Matches
                        matches = df_ref[df_ref[real_name_col].apply(check_token_match)]
                        
                        if not matches.empty:
                            st.markdown(f'<div class="reference-box"><strong>⚠️ Found {len(matches)} potential match(es) in Reference List:</strong></div>', unsafe_allow_html=True)
                            
                            # If multiple matches, let user pick
                            options = matches.apply(lambda x: f"{x[real_name_col]} | {x[real_phone_col]}", axis=1).tolist()
                            selected_option = st.selectbox("Select number to use:", options)
                            
                            # Extract phone from selection
                            if st.button("⬇️ Use Selected Number"):
                                extracted_phone = selected_option.split("|")[-1].strip()
                                st.session_state['temp_filled_phone'] = extracted_phone
                                st.rerun()

            # Determine value to show in text input
            default_phone = st.session_state.pop('temp_filled_phone', current_phone_val)
            
            c6, c7 = st.columns(2)
            new_phone = c6.text_input("Phone", default_phone)
            
            # Row 4: Emails (TP and SP)
            c8, c9 = st.columns(2)
            new_tp_email = c8.text_input("Taxpayer Email", client.get('Taxpayer E-mail Address'))
            new_sp_email = c9.text_input("Spouse Email", client.get('Spouse E-mail Address'))

        # --- NOTES ---
        st.markdown("### Notes")
        st.text_area("History", str(client.get('Notes', '')), disabled=True, height=100)
        new_note = st.text_area("Add Note")

        # --- OUTCOME ---
        c_out1, c_out2 = st.columns(2)
        res = c_out1.selectbox("Result", ["Updated File", "Left Message", "Talked", "Wrong Number"], index=0 if client['Status'] not in ["Left Message", "Talked", "Wrong Number"] else ["Left Message", "Talked", "Wrong Number"].index(client['Status']))
        dec = c_out2.selectbox("Decision", ["Pending", "Yes", "No", "Maybe"], index=["Pending", "Yes", "No", "Maybe"].index(client['Outcome']) if client['Outcome'] in ["Pending", "Yes", "No", "Maybe"] else 0)
        flag = st.checkbox("🚩 Internal Flag", value=(str(client.get('Internal_Flag')) == 'TRUE'))

        # --- EMAIL SECTION ---
        st.markdown("### ✉️ Email Composition")
        
        email_targets = {}
        if new_tp_email: email_targets[f"Taxpayer: {new_tp_email}"] = new_tp_email
        if new_sp_email: email_targets[f"Spouse: {new_sp_email}"] = new_sp_email
        
        enable_email = st.checkbox("Send Email")
        selected_email_address = None
        final_html = ""
        final_text = ""
        subj = ""

        if enable_email:
            if not email_targets:
                st.error("No email addresses found for this client.")
            else:
                if len(email_targets) > 1:
                    target_label = st.radio("Recipient:", list(email_targets.keys()))
                    selected_email_address = email_targets[target_label]
                    is_spouse_email = (selected_email_address == new_sp_email)
                else:
                    target_label = list(email_targets.keys())[0]
                    st.caption(f"Sending to {target_label}")
                    selected_email_address = list(email_targets.values())[0]
                    is_spouse_email = (selected_email_address == new_sp_email)

                ec1, ec2 = st.columns([1, 1])
                tmplt = ec1.selectbox("Template", templates['Type'].unique())
                greeting_style = ec2.radio("Greeting Style", ["Casual", "Formal"], horizontal=True)

                if not templates.empty and tmplt:
                    raw_body = templates[templates['Type'] == tmplt]['Body'].values[0]
                    subj = templates[templates['Type'] == tmplt]['Subject'].values[0]
                    
                    if is_spouse_email and new_sp_first:
                        g_first, g_last = new_sp_first, new_sp_last
                        g_gender = "Unknown" 
                    else:
                        g_first, g_last, g_gender = new_tp_first, new_tp_last, new_gender

                    greeting_line = generate_greeting(greeting_style, g_first, g_last, g_gender)
                    full_body_draft = f"{greeting_line}\n\n{raw_body}"
                    
                    tab_edit, tab_preview = st.tabs(["✏️ Write", "👁️ Preview"])
                    with tab_edit:
                        body_edit = st.text_area("Edit Message Body (HTML supported)", value=full_body_draft, height=300)
                    
                    sig = get_user_signature()
                    final_text = body_edit
                    final_html = f"{body_edit.replace(chr(10), '<br>')}<br><br>{sig}"
                    
                    with tab_preview:
                        st.caption("This is how the client will see it:")
                        st.components.v1.html(final_html, height=300, scrolling=True)

        col_b1, col_b2 = st.columns([1,4])
        
        if col_b1.button("⬅️ Cancel / Back"):
            st.session_state.current_id = None
            st.session_state.admin_current_id = None
            st.rerun()

        if col_b2.button("💾 SAVE & FINISH", type="primary", use_container_width=True):
            # Update
            df.at[idx, 'Taxpayer First Name'] = new_tp_first
            df.at[idx, 'Spouse First Name'] = new_sp_first
            df.at[idx, 'Taxpayer last name'] = new_tp_last
            df.at[idx, 'Spouse last name'] = new_sp_last
            df.at[idx, 'Home Telephone'] = new_phone
            df.at[idx, 'Taxpayer E-mail Address'] = new_tp_email
            df.at[idx, 'Spouse E-mail Address'] = new_sp_email
            df.at[idx, 'Gender'] = new_gender
            df.at[idx, 'Status'] = res
            df.at[idx, 'Outcome'] = dec
            df.at[idx, 'Internal_Flag'] = "TRUE" if flag else "FALSE"
            df.at[idx, 'Last_Agent'] = st.session_state.user_email
            df.at[idx, 'Last_Updated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            
            if new_note:
                df.at[idx, 'Notes'] = str(client.get('Notes', '')) + f"\n[{st.session_state.user_email}]: {new_note}"

            if enable_email and selected_email_address:
                if send_email_as_user(selected_email_address, subj, final_text, final_html):
                    st.toast(f"Email sent to {selected_email_address}")

            with st.spinner("Saving..."):
                update_data(df, "Clients")
            
            if dec == "Yes": st.balloons()
            st.success("Saved!")
            time.sleep(1)
            st.session_state.current_id = None
            st.session_state.admin_current_id = None
            st.rerun()

# ==========================================
# 7. VIEW: TEAM MEMBER (LOBBY vs CARD)
# ==========================================
def render_team_view(df, df_ref, templates, user_email):
    if 'current_id' not in st.session_state: st.session_state.current_id = None
    
    if st.session_state.current_id is None:
        st.subheader("👋 Work Lobby")
        col_queue, col_search = st.columns([1, 1])
        with col_queue:
            with st.container(border=True):
                st.write("### 📞 Call Queue")
                queue = df[df['Status'] == 'New']
                st.metric("Clients Remaining", len(queue))
                if not queue.empty:
                    if st.button("🎲 START RANDOM CALL", type="primary", use_container_width=True):
                        st.session_state.current_id = queue.sample(1).iloc[0]['ID']
                        st.rerun()
                else:
                    st.success("🎉 Queue Complete!")

        with col_search:
            with st.container(border=True):
                st.write("### 🔎 Find Client")
                search = st.text_input("Search Name, Phone, or Email")
                if search:
                    res = df[
                        df['Name'].astype(str).str.contains(search, case=False, na=False) |
                        df['Home Telephone'].astype(str).str.contains(search, case=False, na=False)
                    ]
                    if not res.empty:
                        st.write(f"Found {len(res)}:")
                        for i, row in res.iterrows():
                            c1, c2 = st.columns([3, 1])
                            c1.text(f"{row['Name']} ({row['Status']})")
                            if c2.button("LOAD", key=f"load_{row['ID']}"):
                                st.session_state.current_id = row['ID']
                                st.rerun()
                    else:
                        st.warning("No matches.")
    else:
        render_client_card_editor(df, df_ref, templates, st.session_state.current_id)

# ==========================================
# 8. VIEW: TEMPLATE MANAGER
# ==========================================
def render_template_manager():
    st.subheader("📝 Template Manager")
    df_temp = get_data("Templates")
    col_list, col_editor = st.columns([1, 2])
    
    with col_list:
        st.info("Select a template to edit or add new.")
        options = ["➕ Create New"] + df_temp['Type'].unique().tolist()
        selected_option = st.radio("Available Templates", options)

    with col_editor:
        with st.container(border=True):
            if selected_option == "➕ Create New":
                st.write("### Create New Template")
                t_type = st.text_input("Template Name (Type)")
                t_subj = st.text_input("Subject Line")
                t_body = st.text_area("Body Content (HTML Allowed)", height=300, help="Use <br> for new lines.")
                if st.button("Create Template"):
                    if t_type and t_subj:
                        new_row = pd.DataFrame([[t_type, t_subj, t_body]], columns=['Type', 'Subject', 'Body'])
                        updated_df = pd.concat([df_temp, new_row], ignore_index=True)
                        update_data(updated_df, "Templates")
                        st.success("Created!")
                        st.rerun()
                    else:
                        st.error("Name and Subject required.")
            else:
                st.write(f"### Edit: {selected_option}")
                current_row = df_temp[df_temp['Type'] == selected_option].iloc[0]
                idx = df_temp.index[df_temp['Type'] == selected_option][0]
                new_subj = st.text_input("Subject Line", value=current_row['Subject'])
                t_edit, t_prev = st.tabs(["✏️ Edit HTML", "👁️ Live Preview"])
                with t_edit:
                    new_body = st.text_area("Body Content", value=current_row['Body'], height=300)
                with t_prev:
                    st.markdown("**Preview:**")
                    st.html(f"<div style='background:#f9f9f9; padding:15px; border:1px solid #ddd;'>{new_body.replace(chr(10), '<br>')}</div>")

                if st.button("Update Template", type="primary"):
                    df_temp.at[idx, 'Subject'] = new_subj
                    df_temp.at[idx, 'Body'] = new_body
                    update_data(df_temp, "Templates")
                    st.success("Updated!")

# ==========================================
# 9. VIEW: ADMIN DASHBOARD
# ==========================================
def render_admin_view(df, df_ref, templates, user_email):
    st.title("🔒 Admin Dashboard")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Clients", len(df))
    c2.metric("Calls Made", len(df[df['Status'] != 'New']))
    c3.metric("Pending", len(df[df['Outcome'].isin(['Pending', 'Maybe'])]))
    c4.metric("Success", len(df[df['Outcome'] == 'Yes']))
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Activity", "📥 Inbox", "🔍 Database (Fix)", "📝 Templates"])
    
    with tab1:
        st.subheader("All Call Logs")
        activity = df[df['Status'] != 'New'].sort_values(by='Last_Updated', ascending=False)
        st.dataframe(activity[['Name', 'Status', 'Outcome', 'Last_Updated', 'Last_Agent']])

    with tab2:
            st.subheader("Waiting for Manager Email")
            # Filter for targets
            targets = df[(df['Outcome'] == 'Yes') & (df['Status'] != 'Manager Emailed')]
            
            if targets.empty:
                st.success("🎉 Inbox Zero! No pending manager emails.")
            else:
                # Layout: Left (List) | Right (Compose)
                col_list, col_compose = st.columns([1, 1.5])
                
                with col_list:
                    st.caption(f"Pending: {len(targets)}")
                    # Use selection_mode='single-row' and on_select='rerun' 
                    # to instantly update the right column when clicked
                    event = st.dataframe(
                        targets[['Name', 'Home Telephone', 'Outcome']], 
                        on_select="rerun", 
                        selection_mode="single-row", 
                        use_container_width=True,
                        height=500,
                        key="inbox_list"
                    )
    
                with col_compose:
                    # Check if a row is selected
                    if len(event.selection.rows) > 0:
                        row_idx = event.selection.rows[0]
                        client_id = targets.iloc[row_idx]['ID']
                        # Get fresh client data
                        client = df[df['ID'] == client_id].iloc[0]
                        
                        with st.container(border=True):
                            st.markdown(f"### 📤 Compose: {client['Name']}")
                            st.caption(f"Phone: {client['Home Telephone']} | Email: {client['Taxpayer E-mail Address']}")
                            
                            # --- 1. GENDER & GREETING DEFAULTS ---
                            c_g1, c_g2 = st.columns(2)
                            
                            # Gender logic
                            client_gender = client.get('Gender', 'Unknown')
                            if client_gender not in ["Male", "Female", "Unknown"]: client_gender = "Unknown"
                            conf_gender = c_g1.selectbox("Confirm Gender", ["Male", "Female", "Unknown"], index=["Male", "Female", "Unknown"].index(client_gender))
                            
                            # DEFAULT: Set to 'Formal' automatically (Index 1)
                            greeting_style = c_g2.radio("Greeting Style", ["Casual", "Formal"], index=1, horizontal=True)
    
                            # --- 2. TEMPLATE SELECTION DEFAULT ---
                            if not templates.empty:
                                t_options = templates['Type'].unique().tolist()
                                
                                # DEFAULT: Look for 'Ali - Follow Up'
                                # If it exists, use it. If not, default to 0.
                                target_template = "Ali - Follow Up"
                                default_idx = 0
                                
                                # Case-insensitive search for your preferred template
                                for i, opt in enumerate(t_options):
                                    if target_template.lower() in opt.lower():
                                        default_t_idx = i
                                        break
                                
                                selected_template = st.selectbox("Select Template", t_options, index=default_idx)
                                
                                # Generate Content
                                t_row = templates[templates['Type'] == selected_template].iloc[0]
                                f_name = clean_text(client.get('Taxpayer First Name')) or "Client"
                                l_name = clean_text(client.get('Taxpayer last name'))
                                
                                greeting_line = generate_greeting(greeting_style, f_name, l_name, conf_gender)
                                full_body = f"{greeting_line}\n\n{t_row['Body']}"
                                subj = t_row['Subject']
                                
                                final_subj = st.text_input("Subject", value=subj)
                                final_text = st.text_area("Message Body", value=full_body, height=300)
                                
                                # Preview Toggle
                                with st.expander("👁️ Preview Email"):
                                    sig = get_user_signature()
                                    final_html = f"{final_text.replace(chr(10), '<br>')}<br><br>{sig}"
                                    st.components.v1.html(final_html, height=200, scrolling=True)
    
                                col_send, col_skip = st.columns([2,1])
                                
                                if col_send.button("🚀 SEND & ARCHIVE", type="primary", use_container_width=True):
                                    recipient = client['Taxpayer E-mail Address']
                                    if not recipient:
                                        st.error("Client has no email address!")
                                    else:
                                        # Send logic
                                        if send_email_as_user(recipient, final_subj, final_text, final_html):
                                            idx = df.index[df['ID'] == client['ID']][0]
                                            df.at[idx, 'Status'] = "Manager Emailed"
                                            df.at[idx, 'Gender'] = conf_gender
                                            update_data(df, "Clients")
                                            st.toast(f"✅ Sent to {client['Name']}!")
                                            time.sleep(1)
                                            st.rerun()
                                            
                                if col_skip.button("❌ Deselect"):
                                    st.rerun()
                    else:
                        st.info("👈 Select a client from the list to view the email composer.")
                        st.markdown("Use this list to rapidly process pending manager emails.")

    with tab3: 
        st.subheader("Database Search & Edit")
        # --- FEATURE 2: SPLIT VIEW TO PREVENT JUMPING ---
        col_search, col_admin_edit = st.columns([1, 2])
        
        with col_search:
            st.write("### 🔎 List")
            # Persist search query
            search_query = st.text_input("Search", key="admin_search_query")
            
            if search_query:
                res = df[df['Name'].astype(str).str.contains(search_query, case=False, na=False)]
                if not res.empty:
                    st.caption(f"Found {len(res)} results:")
                    for i, row in res.iterrows():
                        # Use a small container per result
                        with st.container(border=True):
                            st.markdown(f"**{row['Name']}**")
                            st.caption(f"Status: {row['Status']}")
                            if st.button("EDIT", key=f"admin_load_{row['ID']}", use_container_width=True):
                                st.session_state.admin_current_id = row['ID']
                                # No rerun needed here specifically if we just want to update the right column
                else:
                    st.warning("No matches.")
            else:
                st.info("Start typing to search.")

        with col_admin_edit:
            st.write("### 📝 Editor")
            if st.session_state.get('admin_current_id'):
                render_client_card_editor(df, df_ref, templates, st.session_state.admin_current_id)
            else:
                st.caption("Select a client from the list on the left to edit.")

    with tab4:
        render_template_manager()

# ==========================================
# 10. MAIN ROUTER
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
    with st.sidebar:
        user_name = st.session_state.get('user_name', user_email)
        st.write(f"👤 **{user_name}**")
        st.caption(f"Role: {role}")
        
        if st.button("🔄 Refresh Data"):
            get_data.clear()
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.write("**Reference Data Status:**")
        
        try:
            df_ref_check = get_data("Reference")
            if df_ref_check.empty:
                st.error("❌ Reference Tab not found or empty.")
                st.info("Ensure tab is named 'Reference' (case sensitive).")
            else:
                st.success(f"✅ Loaded {len(df_ref_check)} rows.")
        except Exception as e:
            st.error(f"Error loading ref: {e}")

        if st.button("Logout"):
            del st.session_state.creds; del st.session_state.user_email; st.rerun()
            
    df = get_data("Clients")
    # Load Reference Data
    df_ref = get_data("Reference")
    templates = get_data("Templates")
    
    render_gamification(df)
    st.markdown("---")
    if role == "Admin":
        render_admin_view(df, df_ref, templates, user_email)
    else:
        render_team_view(df, df_ref, templates, user_email)
