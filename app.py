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
import os

# ==========================================
# 1. CONFIG
# ==========================================
st.set_page_config(page_title="Kohani CRM", page_icon="📊", layout="wide")
st.markdown("""
    <style>
    #MainMenu {display: none;}
    header {visibility: hidden;}
    div.stButton > button:first-child {
        background-color: #004B87; color: white; border-radius: 8px; font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

# Scopes needed for User Login (Send email + Read signature)
SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.settings.basic',
    'openid', 'https://www.googleapis.com/auth/userinfo.email'
]

# ==========================================
# 2. OAUTH / LOGIN FUNCTIONS
# ==========================================
def get_auth_flow():
    """Creates the OAuth flow object using secrets"""
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
    """Handles the login logic (Redirect to Google)"""
    # 1. If we have a code in URL, exchange it for credentials
    if "code" in st.query_params:
        code = st.query_params["code"]
        flow = get_auth_flow()
        flow.fetch_token(code=code)
        st.session_state.creds = flow.credentials
        
        # Get User Info
        user_info_service = build('oauth2', 'v2', credentials=st.session_state.creds)
        user_info = user_info_service.userinfo().get().execute()
        st.session_state.user_email = user_info.get('email')
        
        # Clear the code from URL to clean up
        st.query_params.clear()
        st.rerun()

    # 2. If valid creds exist, return True
    if "creds" in st.session_state:
        # Check expiry
        if st.session_state.creds.expired and st.session_state.creds.refresh_token:
            st.session_state.creds.refresh(Request())
        return True

    return False

def get_gmail_service():
    if "creds" not in st.session_state: return None
    return build('gmail', 'v1', credentials=st.session_state.creds)

def get_user_signature():
    """Fetches the user's HTML signature from Gmail settings"""
    try:
        service = get_gmail_service()
        # Get primary alias
        sendas_list = service.users().settings().sendAs().list(userId='me').execute()
        for alias in sendas_list.get('sendAs', []):
            if alias.get('isPrimary'):
                return alias.get('signature', '') # This returns HTML
        return ""
    except Exception as e:
        st.warning(f"Could not fetch signature: {e}")
        return ""

def send_email_as_user(to_email, subject, body_html):
    """Sends email using the LOGGED IN user's account via API"""
    try:
        service = get_gmail_service()
        
        message = MIMEMultipart()
        message['to'] = to_email
        message['subject'] = subject
        
        # Attach HTML body (required for signature)
        msg = MIMEText(body_html, 'html')
        message.attach(msg)
        
        # Encode
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        body = {'raw': raw}
        
        service.users().messages().send(userId='me', body=body).execute()
        return True
    except Exception as e:
        st.error(f"Gmail API Error: {e}")
        return False

# ==========================================
# 3. DATABASE CONNECTION (Service Account)
# ==========================================
def get_db_client():
    if "connections" not in st.secrets:
        st.error("❌ Missing DB secrets.")
        st.stop()
    
    secrets_dict = dict(st.secrets["connections"]["gsheets"])
    if "private_key" in secrets_dict:
        secrets_dict["private_key"] = secrets_dict["private_key"].replace("\\n", "\n")
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(secrets_dict, scopes=scopes)
    return gspread.authorize(creds)

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
            st.error(f"Tab '{worksheet_name}' not found.")
            return pd.DataFrame()
            
        raw_data = ws.get_all_values()
        if not raw_data: return pd.DataFrame()
        
        headers = raw_data[0]
        unique_headers = []
        seen = {}
        for h in headers:
            clean_h = str(h).strip()
            if clean_h in seen: seen[clean_h] += 1; unique_headers.append(f"{clean_h}_{seen[clean_h]}")
            else: seen[clean_h] = 0; unique_headers.append(clean_h)
        
        df = pd.DataFrame(raw_data[1:], columns=unique_headers)
        
        if worksheet_name == "Clients":
            for col in ['Status', 'Outcome', 'Internal_Flag', 'Notes', 'Last_Agent', 'Last_Updated']:
                if col not in df.columns: df[col] = ""
            df['Status'] = df['Status'].replace("", "New")
            
        return df
    except Exception as e:
        st.error(f"DB Error: {e}")
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
    except Exception as e:
        st.error(f"Save Error: {e}")

def clean_text(text):
    if not text: return ""
    return str(text).title().strip()

# ==========================================
# 4. UI LOGIC
# ==========================================
if not authenticate_user():
    # --- LOGIN SCREEN ---
    c1, c2, c3 = st.columns([1,2,1])
    with c2:
        st.image("https://kohani.com/wp-content/uploads/2015/05/logo.png", width=200)
        st.title("Kohani CRM Login")
        st.info("Please log in with your @kohani.com Gmail account.")
        
        flow = get_auth_flow()
        auth_url, _ = flow.authorization_url(prompt='consent')
        
        st.link_button("🔵 Sign in with Google", auth_url, type="primary")
else:
    # --- LOGGED IN APP ---
    # Determine Role (Admin Check)
    user_email = st.session_state.user_email
    role = "Admin" if ("ali" in user_email or "admin" in user_email) else "Staff"

    # --- SIDEBAR ---
    with st.sidebar:
        st.write(f"👤 **{user_email}**")
        if st.button("Logout"):
            del st.session_state.creds
            del st.session_state.user_email
            st.rerun()

    # --- LOAD DATA ---
    df = get_data("Clients")
    templates = get_data("Templates")

    # --- ADMIN VIEW ---
    if role == "Admin":
        st.title("🔒 Admin Dashboard")
        st.metric("Total Clients", len(df))
        st.metric("Calls Done", len(df[df['Status'] != 'New']))
        st.subheader("Inbox (Yes + Pending Email)")
        targets = df[(df['Outcome'] == 'Yes') & (df['Status'] != 'Manager Emailed')]
        st.dataframe(targets)
    
    # --- STAFF VIEW ---
    else:
        with st.sidebar:
            st.markdown("---")
            search = st.text_input("Search Name/Phone")
            if search:
                res = df[df['Name'].astype(str).str.contains(search, case=False, na=False)]
                if not res.empty:
                    st.write(f"Found {len(res)}")
                    label = st.selectbox("Select", res['Name'] + " (" + res['Status'] + ")")
                    if st.button("Load"):
                        t_id = res[res['Name'] + " (" + res['Status'] + ")" == label]['ID'].values[0]
                        st.session_state.current_id = t_id
                        st.rerun()

        if 'current_id' not in st.session_state: st.session_state.current_id = None
        
        if st.session_state.current_id is None:
            queue = df[df['Status'] == 'New']
            st.title("📞 Call Queue")
            st.info(f"Remaining: {len(queue)}")
            if not queue.empty and st.button("START CALL", type="primary"):
                st.session_state.current_id = queue.sample(1).iloc[0]['ID']
                st.rerun()
        else:
            # Active Card
            mask = df['ID'] == st.session_state.current_id
            if not mask.any(): st.session_state.current_id = None; st.rerun()
            idx = df.index[mask][0]
            client = df.loc[idx]

            with st.container(border=True):
                st.title(clean_text(client['Name']))
                
                # Edit Form
                with st.expander("📝 Edit Details", expanded=True):
                    c1, c2 = st.columns(2)
                    new_tp_first = c1.text_input("TP First Name", clean_text(client.get('Taxpayer First Name')))
                    new_sp_first = c2.text_input("SP First Name", clean_text(client.get('Spouse First Name')))
                    c3, c4 = st.columns(2)
                    new_phone = c3.text_input("Phone", client.get('Home Telephone'))
                    new_email = c4.text_input("Email", client.get('Taxpayer E-mail Address'))

                # Notes
                st.markdown("### Notes")
                st.text_area("History", str(client.get('Notes','')), disabled=True, height=100)
                new_note = st.text_area("Add Note")

                # Outcome
                c_out1, c_out2 = st.columns(2)
                res = c_out1.selectbox("Result", ["Left Message", "Talked", "Wrong Number"])
                dec = c_out2.selectbox("Decision", ["Pending", "Yes", "No"])
                
                # Email with Signature
                send_email = st.checkbox(f"Email {new_email}")
                final_html = ""
                subj = ""
                
                if send_email:
                    if not templates.empty:
                        tmplt = st.selectbox("Template", templates['Type'].unique())
                        raw_body = templates[templates['Type'] == tmplt]['Body'].values[0]
                        subj = templates[templates['Type'] == tmplt]['Subject'].values[0]
                        
                        # Fetch Signature
                        sig = get_user_signature()
                        
                        # Replace logic (convert newlines to <br> for HTML)
                        disp_name = new_tp_first if new_tp_first else "Client"
                        body_content = raw_body.replace("{Name}", disp_name).replace("\n", "<br>")
                        
                        # Combine Body + Signature
                        final_html = f"{body_content}<br><br>{sig}"
                        
                        # Preview
                        st.markdown("---")
                        st.caption("📧 Email Preview:")
                        st.html(final_html) # Streamlit command to render HTML
                    else:
                        st.warning("No templates.")

                # Save
                if st.button("💾 SAVE & FINISH", type="primary"):
                    # Update DF logic...
                    df.at[idx, 'Taxpayer First Name'] = new_tp_first
                    df.at[idx, 'Spouse First Name'] = new_sp_first
                    df.at[idx, 'Home Telephone'] = new_phone
                    df.at[idx, 'Taxpayer E-mail Address'] = new_email
                    df.at[idx, 'Status'] = res
                    df.at[idx, 'Outcome'] = dec
                    df.at[idx, 'Last_Agent'] = user_email
                    df.at[idx, 'Last_Updated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    
                    if new_note:
                        df.at[idx, 'Notes'] = str(client.get('Notes','')) + f"\n[{user_email}]: {new_note}"

                    if send_email and new_email:
                        if send_email_as_user(new_email, subj, final_html):
                            st.toast("Email Sent")

                    update_data(df, "Clients")
                    st.success("Saved!")
                    time.sleep(1)
                    st.session_state.current_id = None
                    st.rerun()
