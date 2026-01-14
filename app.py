
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
    .email-row {
        padding: 10px;
        border-bottom: 1px solid #eee;
    }
    .email-date { font-size: 0.8em; color: #666; }
    .email-subject { font-weight: bold; color: #004B87; }
    .email-snippet { font-size: 0.9em; color: #333; }
    </style>
    """, unsafe_allow_html=True)

# Admin Email for CC
ADMIN_EMAIL = "ali@kohani.com"

SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly', # Added for searching inbox
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

def search_gmail_messages(query_emails):
    """
    Searches the logged-in user's Gmail for emails to/from the provided list.
    Returns a list of dicts: {id, threadId, subject, snippet, date}
    """
    if not query_emails: return []
    try:
        service = get_gmail_service()
        # Construct query: from:a@b.com OR to:a@b.com
        q_parts = [f"from:{e} OR to:{e}" for e in query_emails if e and "@" in e]
        if not q_parts: return []
        
        full_query = " OR ".join(q_parts)
        
        results = service.users().messages().list(userId='me', q=full_query, maxResults=10).execute()
        messages = results.get('messages', [])
        
        email_data = []
        if messages:
            for msg in messages:
                m_detail = service.users().messages().get(userId='me', id=msg['id'], format='metadata').execute()
                headers = m_detail.get('payload', {}).get('headers', [])
                
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '(No Subject)')
                date_str = next((h['value'] for h in headers if h['name'] == 'Date'), '')
                snippet = m_detail.get('snippet', '')
                
                email_data.append({
                    'subject': subject,
                    'date': date_str,
                    'snippet': snippet
                })
        return email_data
    except Exception as e:
        # Fail silently or return empty if permission denied/error
        # print(e) 
        return []

def send_email_as_user(to_email, subject, body_text, body_html):
    try:
        service = get_gmail_service()
        message = MIMEMultipart('alternative')
        
        sender_header = f"{st.session_state.user_name} <{st.session_state.user_email}>"
        message['to'] = to_email
        message['from'] = sender_header 
        
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
# 3. DATABASE FUNCTIONS & HELPERS
# ==========================================
def normalize_phone(phone):
    """Strips everything except digits to allow loose matching."""
    if not phone: return ""
    return re.sub(r'\D', '', str(phone))

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
    
    if 'Last_Updated' in df.columns:
        daily_df = df[df['Last_Updated'].str.contains(today_str, na=False)]
    else:
        daily_df = pd.DataFrame()
        
    my_calls = len(daily_df[daily_df['Last_Agent'] == st.session_state.user_email])
    total_daily = len(daily_df)
    
    target = 50 
    progress = min(my_calls / target, 1.0)
    
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        st.metric("📞 My Calls Today", f"{my_calls} / {target}")
        st.progress(progress)
        if my_calls >= target and "goal_celebrated" not in st.session_state:
            st.balloons()
            st.session_state.goal_celebrated = True
        
    with c2:
        st.metric("🌍 Team Total Today", total_daily)
    
    with c3:
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
# 6. CLIENT CARD EDITOR
# ==========================================
def render_client_card_editor(df, df_ref, templates, client_id):
    # Isolate Client
    idx = df.index[df['ID'] == client_id][0]
    client = df.loc[idx]
    
    # ------------------------------------
    # MANUAL REFERENCE SEARCH UI (Feature #3)
    # ------------------------------------
    if not df_ref.empty:
        with st.sidebar.expander("🔎 Manual Reference Search", expanded=False):
            st.caption("Check the Reference sheet for contact info.")
            ref_search = st.text_input("Type name or phone:", key="manual_ref_search")
            if ref_search:
                # Basic search across all columns in Ref
                mask = df_ref.apply(lambda x: x.astype(str).str.contains(ref_search, case=False, na=False)).any(axis=1)
                ref_hits = df_ref[mask]
                if not ref_hits.empty:
                    st.write(f"Found {len(ref_hits)} matches:")
                    for _, r_row in ref_hits.iterrows():
                        # Try to display something useful
                        disp_str = " | ".join([str(val) for val in r_row.values if str(val)])
                        st.text_area("Match", disp_str[:200], height=60)
                else:
                    st.warning("No matches in Reference sheet.")

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
            
            current_phone_val = client.get('Home Telephone', '')
            
            # --- AUTO REFERENCE MATCHING ---
            found_ref_phone = None
            if (not current_phone_val or len(str(current_phone_val)) < 5) and not df_ref.empty:
                # Dynamic Column Identification
                ref_cols = [str(c).lower().strip() for c in df_ref.columns]
                phone_keywords = ['phone', 'mobile', 'cell', 'tel', 'contact', 'number']
                name_keywords = ['name', 'client', 'customer', 'taxpayer', 'person']
                
                phone_col_idx = -1
                for i, col_name in enumerate(ref_cols):
                    if any(k in col_name for k in phone_keywords): phone_col_idx = i; break
                name_col_idx = -1
                for i, col_name in enumerate(ref_cols):
                    if any(k in col_name for k in name_keywords): name_col_idx = i; break

                if phone_col_idx != -1 and name_col_idx != -1:
                    real_phone_col = df_ref.columns[phone_col_idx]
                    real_name_col = df_ref.columns[name_col_idx]
                    
                    client_name_str = str(client['Name']).lower()
                    client_tokens = set(re.findall(r'\w+', client_name_str))

                    if client_tokens:
                        def check_token_match(ref_val):
                            if not isinstance(ref_val, str): return False
                            ref_tokens = set(re.findall(r'\w+', ref_val.lower()))
                            if not ref_tokens: return False
                            common = client_tokens.intersection(ref_tokens)
                            if len(common) >= 2: return True
                            if ref_tokens.issubset(client_tokens): return True
                            if client_tokens.issubset(ref_tokens): return True
                            return False

                        matches = df_ref[df_ref[real_name_col].apply(check_token_match)]
                        
                        if not matches.empty:
                            st.markdown(f'<div class="reference-box"><strong>⚠️ Found {len(matches)} potential match(es) in Reference List:</strong></div>', unsafe_allow_html=True)
                            options = matches.apply(lambda x: f"{x[real_name_col]} | {x[real_phone_col]}", axis=1).tolist()
                            selected_option = st.selectbox("Select number to use:", options)
                            if st.button("⬇️ Use Selected Number"):
                                extracted_phone = selected_option.split("|")[-1].strip()
                                st.session_state['temp_filled_phone'] = extracted_phone
                                st.rerun()

            default_phone = st.session_state.pop('temp_filled_phone', current_phone_val)
            c6, c7 = st.columns(2)
            new_phone = c6.text_input("Phone", default_phone)
            
            # Row 4: Emails
            c8, c9 = st.columns(2)
            new_tp_email = c8.text_input("Taxpayer Email", client.get('Taxpayer E-mail Address'))
            new_sp_email = c9.text_input("Spouse Email", client.get('Spouse E-mail Address'))

        # --- TABS: HISTORY & EMAILS ---
        tab_notes, tab_email, tab_gmail_hist = st.tabs(["📝 Notes / History", "✉️ Compose Email", "📧 Gmail History"])

        with tab_notes:
            st.text_area("History Log", str(client.get('Notes', '')), disabled=True, height=150)
            new_note = st.text_area("Add Note")

        with tab_gmail_hist:
            # Feature #4: Unified-ish Inbox (Search logged-in user's gmail)
            st.caption("Searching your Gmail for correspondence with this client...")
            search_targets = []
            if new_tp_email: search_targets.append(new_tp_email)
            if new_sp_email: search_targets.append(new_sp_email)
            
            if not search_targets:
                st.info("No email addresses on file to search.")
            else:
                gmail_results = search_gmail_messages(search_targets)
                if gmail_results:
                    for msg in gmail_results:
                        st.markdown(f"""
                        <div class="email-row">
                            <div class="email-date">{msg['date']}</div>
                            <div class="email-subject">{msg['subject']}</div>
                            <div class="email-snippet">{msg['snippet']}</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.write("No emails found in your inbox for these addresses.")

        with tab_email:
            st.caption("Feature: Emails sent here are automatically logged to Notes.")
            email_targets = {}
            if new_tp_email: email_targets[f"Taxpayer: {new_tp_email}"] = new_tp_email
            if new_sp_email: email_targets[f"Spouse: {new_sp_email}"] = new_sp_email
            
            enable_email = st.checkbox("Send Email Now")
            selected_email_address = None
            final_html = ""
            final_text = ""
            subj = ""

            if enable_email:
                if not email_targets:
                    st.error("No email addresses found.")
                else:
                    if len(email_targets) > 1:
                        target_label = st.radio("Recipient:", list(email_targets.keys()))
                        selected_email_address = email_targets[target_label]
                        is_spouse_email = (selected_email_address == new_sp_email)
                    else:
                        target_label = list(email_targets.keys())[0]
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
                        
                        body_edit = st.text_area("Edit Message Body", value=full_body_draft, height=200)
                        
                        sig = get_user_signature()
                        final_text = body_edit
                        final_html = f"{body_edit.replace(chr(10), '<br>')}<br><br>{sig}"
                        
                        st.markdown("**Preview:**")
                        st.components.v1.html(final_html, height=200, scrolling=True)

        # --- OUTCOME ---
        st.markdown("---")
        c_out1, c_out2 = st.columns(2)
        
        # Determine Status Index
        status_opts = ["Updated File", "Left Message", "Talked", "Wrong Number"]
        curr_stat = client['Status']
        if curr_stat not in status_opts: 
            # If current status is weird (like "New" or "Manager Emailed"), default to first or keep weird if not in list
            stat_idx = 0 
        else:
            stat_idx = status_opts.index(curr_stat)

        res = c_out1.selectbox("Call Result", status_opts, index=stat_idx)
        
        outcome_opts = ["Pending", "Yes", "No", "Maybe"]
        curr_out = client['Outcome']
        dec_idx = outcome_opts.index(curr_out) if curr_out in outcome_opts else 0
        dec = c_out2.selectbox("Decision", outcome_opts, index=dec_idx)
        
        flag = st.checkbox("🚩 Internal Flag", value=(str(client.get('Internal_Flag')) == 'TRUE'))

        # --- ACTIONS ---
        col_b1, col_b2 = st.columns([1,4])
        
        if col_b1.button("⬅️ Cancel"):
            st.session_state.current_id = None
            st.session_state.admin_current_id = None
            st.rerun()

        if col_b2.button("💾 SAVE & FINISH", type="primary", use_container_width=True):
            # Update Data
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
            
            # Combine notes
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            note_append = ""
            if new_note:
                note_append += f"\n[{timestamp} {st.session_state.user_email}]: {new_note}"

            if enable_email and selected_email_address:
                if send_email_as_user(selected_email_address, subj, final_text, final_html):
                    st.toast(f"Email sent to {selected_email_address}")
                    # Feature #4 (Option B): Log Email to Notes
                    note_append += f"\n[EMAIL SENT by {st.session_state.user_email}]\nSubject: {subj}\nTo: {selected_email_address}\n"
            
            if note_append:
                df.at[idx, 'Notes'] = str(client.get('Notes', '')) + note_append

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
                
                # Feature #2: Ensure we only pick explicitly 'New' statuses to avoid recalling people
                queue = df[df['Status'] == 'New']
                st.metric("New Leads Remaining", len(queue))
                
                if not queue.empty:
                    if st.button("🎲 START CALL (Prioritize Phones)", type="primary", use_container_width=True):
                        # Feature #1: Prioritize Phone Numbers
                        # Check if phone has at least 7 digits
                        queue['clean_phone'] = queue['Home Telephone'].apply(normalize_phone)
                        with_phone = queue[queue['clean_phone'].str.len() > 6]
                        no_phone = queue[queue['clean_phone'].str.len() <= 6]
                        
                        if not with_phone.empty:
                            selected = with_phone.sample(1).iloc[0]
                        else:
                            selected = no_phone.sample(1).iloc[0]
                            
                        st.session_state.current_id = selected['ID']
                        st.rerun()
                else:
                    st.success("🎉 Queue Complete!")

        with col_search:
            with st.container(border=True):
                st.write("### 🔎 Find Client (Deep Search)")
                search = st.text_input("Search Name, Phone, Email, or Notes")
                if search:
                    # Feature #5: Normalize search term for phone logic + Search Notes
                    search_norm = normalize_phone(search)
                    
                    # Create normalized phone col for search purposes only
                    df['search_phone'] = df['Home Telephone'].apply(normalize_phone)
                    
                    # Logic: 
                    # 1. Text match on Name/Email/Notes
                    # 2. IF search term has digits, compare against normalized phone
                    
                    mask = (
                        df['Name'].astype(str).str.contains(search, case=False, na=False) |
                        df['Taxpayer E-mail Address'].astype(str).str.contains(search, case=False, na=False) |
                        df['Notes'].astype(str).str.contains(search, case=False, na=False)
                    )
                    
                    if len(search_norm) > 4:
                        mask = mask | df['search_phone'].str.contains(search_norm, na=False)
                    
                    res = df[mask]

                    if not res.empty:
                        st.write(f"Found {len(res)}:")
                        for i, row in res.iterrows():
                            c1, c2 = st.columns([3, 1])
                            c1.text(f"{row['Name']} | {row['Home Telephone']} | {row['Status']}")
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
                t_body = st.text_area("Body Content (HTML Allowed)", height=300)
                if st.button("Create Template"):
                    if t_type and t_subj:
                        new_row = pd.DataFrame([[t_type, t_subj, t_body]], columns=['Type', 'Subject', 'Body'])
                        updated_df = pd.concat([df_temp, new_row], ignore_index=True)
                        update_data(updated_df, "Templates")
                        st.success("Created!")
                        st.rerun()
            else:
                st.write(f"### Edit: {selected_option}")
                current_row = df_temp[df_temp['Type'] == selected_option].iloc[0]
                idx = df_temp.index[df_temp['Type'] == selected_option][0]
                new_subj = st.text_input("Subject Line", value=current_row['Subject'])
                t_edit, t_prev = st.tabs(["✏️ Edit HTML", "👁️ Live Preview"])
                with t_edit:
                    new_body = st.text_area("Body Content", value=current_row['Body'], height=300)
                with t_prev:
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
    
    if "admin_nav" not in st.session_state: st.session_state.admin_nav = "📥 Inbox"
    nav_options = ["📊 Activity", "📥 Inbox", "🔍 Database (Fix)", "📝 Templates"]
    
    selected_view = st.radio("Admin Navigation", nav_options, index=nav_options.index(st.session_state.admin_nav), horizontal=True, label_visibility="collapsed", key="admin_nav_radio", on_change=lambda: st.session_state.update(admin_nav=st.session_state.admin_nav_radio))
    st.session_state.admin_nav = selected_view
    st.markdown("---")

    if selected_view == "📊 Activity":
        st.subheader("All Call Logs")
        activity = df[df['Status'] != 'New'].sort_values(by='Last_Updated', ascending=False)
        st.dataframe(activity[['Name', 'Status', 'Outcome', 'Last_Updated', 'Last_Agent']], use_container_width=True)

    elif selected_view == "📥 Inbox":
        if "skipped_ids" not in st.session_state: st.session_state.skipped_ids = []
        targets = df[(df['Outcome'] == 'Yes') & (df['Status'] != 'Manager Emailed')]
        targets = targets[~targets['ID'].isin(st.session_state.skipped_ids)]
        
        col_header, col_mode = st.columns([2, 1])
        with col_header: st.subheader(f"Waiting for Manager Email ({len(targets)})")
        with col_mode: rapid_mode = st.toggle("⚡ Rapid Review Mode", value=True)

        if targets.empty:
            st.success("🎉 Inbox Zero!")
            if st.session_state.skipped_ids and st.button("Reset Skipped Clients"):
                st.session_state.skipped_ids = []
                st.rerun()
        else:
            current_client = None
            if rapid_mode:
                current_client = targets.iloc[0]
            else:
                col_list, col_compose = st.columns([1, 1.5])
                with col_list:
                    event = st.dataframe(targets[['Name', 'Home Telephone', 'Outcome']], on_select="rerun", selection_mode="single-row", use_container_width=True, height=700)
                    if len(event.selection.rows) > 0:
                        current_client = df[df['ID'] == targets.iloc[event.selection.rows[0]]['ID']].iloc[0]

            if current_client is not None:
                container = st.container() if rapid_mode else col_compose
                with container:
                    if rapid_mode: st.info(f"⚡ processing **{current_client['Name']}**")
                    with st.container(border=True):
                        tp_name = clean_text(current_client.get('Taxpayer First Name'))
                        tp_last = clean_text(current_client.get('Taxpayer last name'))
                        tp_email = str(current_client.get('Taxpayer E-mail Address', '')).strip()
                        sp_name = clean_text(current_client.get('Spouse First Name'))
                        sp_last = clean_text(current_client.get('Spouse last name'))
                        sp_email = str(current_client.get('Spouse E-mail Address', '')).strip()

                        target_map = {"TP": f"Taxpayer: {tp_name}", "SP": f"Spouse: {sp_name}"}
                        options = [target_map["TP"]]
                        if sp_name: options.append(target_map["SP"])
                        
                        selected_label = st.radio("Recipient:", options, horizontal=True, key=f"recip_rad_{current_client['ID']}")
                        is_spouse = (selected_label == target_map.get("SP"))
                        
                        if is_spouse:
                            target_code, f_name, l_name, selected_email_addr, db_gender = "SP", sp_name, sp_last, sp_email, "Unknown"
                        else:
                            target_code, f_name, l_name, selected_email_addr, db_gender = "TP", tp_name, tp_last, tp_email, current_client.get('Gender', 'Unknown')
                            
                        if not selected_email_addr: st.error(f"❌ No email found for {selected_label}.")
                        if db_gender not in ["Male", "Female", "Unknown"]: db_gender = "Unknown"

                        c_g1, c_g2 = st.columns(2)
                        conf_gender = c_g1.selectbox("Gender", ["Male", "Female", "Unknown"], index=["Male", "Female", "Unknown"].index(db_gender), key=f"gender_{current_client['ID']}")
                        greeting_style = c_g2.radio("Style", ["Casual", "Formal"], index=1, horizontal=True, key=f"greet_{current_client['ID']}")

                        if not templates.empty:
                            t_options = templates['Type'].unique().tolist()
                            selected_template = st.selectbox("Template", t_options, index=0, key=f"temp_{current_client['ID']}")
                            t_row = templates[templates['Type'] == selected_template].iloc[0]
                            
                            greeting_line = f"Hi {f_name}," if greeting_style == "Casual" else f"Dear {f_name} {l_name},"
                            if greeting_style == "Formal":
                                if conf_gender == "Male": greeting_line = f"Dear Mr. {l_name},"
                                elif conf_gender == "Female": greeting_line = f"Dear Ms. {l_name},"

                            full_body = f"{greeting_line}\n\n{t_row['Body']}"
                            subj = t_row['Subject']
                            
                            final_subj = st.text_input("Subject", value=subj, key=f"subj_{current_client['ID']}")
                            final_text = st.text_area("Body", value=full_body, height=300, key=f"body_{current_client['ID']}")
                            new_note = st.text_area("Internal Note", height=70, key=f"note_{current_client['ID']}")

                            col_send, col_skip = st.columns([2,1])
                            btn_label = "🚀 SEND & NEXT" if rapid_mode else "🚀 SEND & ARCHIVE"
                            
                            if col_send.button(btn_label, type="primary", use_container_width=True, key=f"btn_send_{current_client['ID']}"):
                                if not selected_email_addr: st.error("No email!")
                                else:
                                    sig = get_user_signature()
                                    final_html = f"{final_text.replace(chr(10), '<br>')}<br><br>{sig}"
                                    if send_email_as_user(selected_email_addr, final_subj, final_text, final_html):
                                        idx = df.index[df['ID'] == current_client['ID']][0]
                                        df.at[idx, 'Status'] = "Manager Emailed"
                                        if target_code == "TP": df.at[idx, 'Gender'] = conf_gender
                                        
                                        # Log to notes
                                        existing_notes = str(df.at[idx, 'Notes'])
                                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                        log_entry = f"[{timestamp} {st.session_state.user_email}]: {new_note}\n[EMAIL SENT] To: {selected_email_addr}"
                                        df.at[idx, 'Notes'] = f"{existing_notes}\n{log_entry}"

                                        update_data(df, "Clients")
                                        st.toast(f"✅ Sent to {f_name}!")
                                        time.sleep(0.5)
                                        st.rerun()
                            
                            if col_skip.button("⏭️ SKIP", key=f"btn_skip_{current_client['ID']}"):
                                st.session_state.skipped_ids.append(current_client['ID'])
                                st.rerun()

    elif selected_view == "🔍 Database (Fix)":
        st.subheader("Database Search & Edit")
        col_search, col_admin_edit = st.columns([1, 2])
        with col_search:
            st.write("### 🔎 List")
            search_query = st.text_input("Search", key="admin_search_query")
            if search_query:
                # Same robust search logic as Lobby
                search_norm = normalize_phone(search_query)
                df['search_phone'] = df['Home Telephone'].apply(normalize_phone)
                
                mask = (
                    df['Name'].astype(str).str.contains(search_query, case=False, na=False) |
                    df['Taxpayer E-mail Address'].astype(str).str.contains(search_query, case=False, na=False) |
                    df['Notes'].astype(str).str.contains(search_query, case=False, na=False)
                )
                if len(search_norm) > 4:
                    mask = mask | df['search_phone'].str.contains(search_norm, na=False)
                    
                res = df[mask]
                if not res.empty:
                    for i, row in res.iterrows():
                        with st.container(border=True):
                            st.markdown(f"**{row['Name']}**\n{row['Home Telephone']}")
                            if st.button("EDIT", key=f"admin_load_{row['ID']}", use_container_width=True):
                                st.session_state.admin_current_id = row['ID']
                else:
                    st.warning("No matches.")
        with col_admin_edit:
            st.write("### 📝 Editor")
            if st.session_state.get('admin_current_id'):
                render_client_card_editor(df, df_ref, templates, st.session_state.admin_current_id)

    elif selected_view == "📝 Templates":
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
        if st.button("Logout"):
            del st.session_state.creds; del st.session_state.user_email; st.rerun()
            
    df = get_data("Clients")
    df_ref = get_data("Reference")
    templates = get_data("Templates")
    
    render_gamification(df)
    st.markdown("---")
    if role == "Admin":
        render_admin_view(df, df_ref, templates, user_email)
    else:
        render_team_view(df, df_ref, templates, user_email)
