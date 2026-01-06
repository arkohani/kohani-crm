import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import socket

# Force timeout for firewalls
socket.setdefaulttimeout(30)

# ==========================================
# 1. CONFIG
# ==========================================
st.set_page_config(page_title="Kohani CRM", page_icon="📊", layout="wide")

# CSS: Hides the default menu but DOES NOT change input colors (fixes the white text issue)
st.markdown("""
    <style>
    #MainMenu {display: none;}
    header {visibility: hidden;}
    div.stButton > button:first-child {
        background-color: #004B87; color: white; border-radius: 8px; font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. GOOGLE CONNECTION
# ==========================================
def get_google_client():
    if "connections" not in st.secrets:
        st.error("❌ Secrets file missing [connections].")
        st.stop()
    
    secrets_dict = dict(st.secrets["connections"]["gsheets"])
    if "private_key" in secrets_dict:
        secrets_dict["private_key"] = secrets_dict["private_key"].replace("\\n", "\n")
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(secrets_dict, scopes=scopes)
    return gspread.authorize(creds)

def get_data(worksheet_name="Clients"):
    try:
        client = get_google_client()
        raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
        sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
        
        sh = client.open_by_key(sheet_id)
        
        try:
            ws = sh.worksheet(worksheet_name)
        except:
            if worksheet_name == "Templates":
                return pd.DataFrame(columns=['Type', 'Subject', 'Body'])
            st.error(f"❌ Tab '{worksheet_name}' not found.")
            return pd.DataFrame()
            
        raw_data = ws.get_all_values()
        if not raw_data: return pd.DataFrame()
        
        headers = raw_data[0]
        rows = raw_data[1:]
        
        # Handle Duplicate Headers
        unique_headers = []
        seen = {}
        for h in headers:
            clean_h = str(h).strip()
            if clean_h in seen:
                seen[clean_h] += 1
                unique_headers.append(f"{clean_h}_{seen[clean_h]}")
            else:
                seen[clean_h] = 0
                unique_headers.append(clean_h)
        
        df = pd.DataFrame(rows, columns=unique_headers)
        
        # Ensure Tracking Columns Exist
        if worksheet_name == "Clients":
            for col in ['Status', 'Outcome', 'Internal_Flag', 'Notes', 'Last_Agent', 'Last_Updated']:
                if col not in df.columns:
                    df[col] = ""
            df['Status'] = df['Status'].replace("", "New")
            
        return df
    except Exception as e:
        st.error(f"Data Load Error: {e}")
        return pd.DataFrame()

def update_data(df, worksheet_name="Clients"):
    try:
        client = get_google_client()
        raw_input = st.secrets["connections"]["gsheets"]["spreadsheet"]
        sheet_id = raw_input.replace("https://docs.google.com/spreadsheets/d/", "").split("/")[0].strip()
        sh = client.open_by_key(sheet_id)
        ws = sh.worksheet(worksheet_name)
        ws.clear()
        params = [df.columns.values.tolist()] + df.values.tolist()
        ws.update(params)
    except Exception as e:
        st.error(f"Save Error: {e}")

# ==========================================
# 3. EMAIL FUNCTION
# ==========================================
def send_email_via_gmail(to_email, subject, body):
    try:
        sender_email = st.secrets["email"]["user"]
        password = st.secrets["email"]["password"]
        admin_email = st.secrets["email"]["admin_email"]
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = to_email
        msg['Cc'] = admin_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg, to_addrs=[to_email, admin_email])
        server.quit()
        return True
    except Exception as e:
        st.error(f"Email Failed: {e}")
        return False

# ==========================================
# 4. SECURE LOGIN
# ==========================================
if 'user' not in st.session_state:
    st.session_state.user = None
if 'role' not in st.session_state:
    st.session_state.role = None

def login_screen():
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.image("https://kohani.com/wp-content/uploads/2015/05/logo.png", width=200)
        st.title("Tax Season CRM")
        
        with st.form("login_form"):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Sign In")
            
            if submit:
                users = st.secrets["users"]
                if email in users and users[email] == password:
                    st.session_state.user = email
                    if "ali" in email or "admin" in email:
                        st.session_state.role = "Admin"
                    else:
                        st.session_state.role = "Staff"
                    st.rerun()
                else:
                    st.error("Invalid Email or Password.")

# ==========================================
# 5. TEAM VIEW
# ==========================================
def render_team_view():
    df = get_data("Clients")
    templates = get_data("Templates")
    
    if df.empty:
        st.warning("Client list is loading or empty...")
        return

    # --- SIDEBAR: NAVIGATION & SEARCH ---
    with st.sidebar:
        st.write(f"👤 **{st.session_state.user}**")
        if st.button("Logout"):
            st.session_state.user = None; st.rerun()
        
        st.markdown("---")
        st.subheader("🔍 Search Database")
        search_term = st.text_input("Find Client (Name/Phone)")
        
        if search_term:
            # Filter results
            res = df[
                df['Name'].astype(str).str.contains(search_term, case=False, na=False) |
                df['Home Telephone'].astype(str).str.contains(search_term, case=False, na=False)
            ]
            if not res.empty:
                st.write(f"Found {len(res)}:")
                # Create label for dropdown
                res['Label'] = res['Name'] + " (" + res['Status'] + ")"
                target_label = st.selectbox("Select Result", res['Label'])
                
                # Get ID from selection
                target_id = res[res['Label'] == target_label]['ID'].values[0]
                
                if st.button("📂 LOAD CLIENT CARD"):
                    st.session_state.current_id = target_id
                    st.rerun()
            else:
                st.warning("No matches.")

        st.markdown("---")
        # Stats
        completed = len(df[df['Status'] != 'New'])
        st.progress(completed / len(df), text=f"{completed}/{len(df)} Calls Done")


    # --- MAIN CONTENT AREA ---
    
    # Check "Current Call" State
    if 'current_id' not in st.session_state:
        st.session_state.current_id = None
        
    # 1. NO ACTIVE CLIENT -> SHOW START BUTTON
    if st.session_state.current_id is None:
        queue = df[df['Status'] == 'New']
        st.title("📞 Call Queue")
        st.info(f"Clients remaining to call: **{len(queue)}**")
        
        if queue.empty:
            st.success("🎉 All new clients called!")
        else:
            if st.button("📞 START NEXT CALL", type="primary"):
                client = queue.sample(1).iloc[0]
                st.session_state.current_id = client['ID']
                st.rerun()

    # 2. ACTIVE CLIENT CARD
    else:
        # Find the row index
        mask = df['ID'] == st.session_state.current_id
        if not mask.any():
            st.session_state.current_id = None
            st.rerun()
        
        idx = df.index[mask][0]
        client = df.loc[idx]
        
        with st.container(border=True):
            # Header
            c_h1, c_h2 = st.columns([3,1])
            c_h1.title(f"{client['Name']}")
            c_h2.metric("Status", client['Status'])
            
            # --- EDITABLE DETAILS ---
            with st.expander("📝 Edit Client Details (Open to Edit)", expanded=True):
                # Using .get() ensures it doesn't crash if column names differ slightly
                c1, c2 = st.columns(2)
                # First Names
                new_tp_first = c1.text_input("Taxpayer First Name", value=client.get('Taxpayer First Name', ''))
                new_sp_first = c2.text_input("Spouse First Name", value=client.get('Spouse First Name', ''))
                
                # Last Names
                c3, c4 = st.columns(2)
                new_tp_last = c3.text_input("Taxpayer Last Name", value=client.get('Taxpayer last name', ''))
                new_sp_last = c4.text_input("Spouse Last Name", value=client.get('Spouse last name', ''))
                
                # Contact
                c5, c6 = st.columns(2)
                new_phone = c5.text_input("Home Telephone", value=client.get('Home Telephone', ''))
                new_email = c6.text_input("Taxpayer Email", value=client.get('Taxpayer E-mail Address', ''))

            # --- NOTES & HISTORY ---
            st.markdown("### 📜 Notes")
            current_notes = str(client.get('Notes', ''))
            st.text_area("History", value=current_notes, disabled=True, height=100)
            new_note_input = st.text_area("➕ Add Call Summary / Note")

            st.markdown("---")

            # --- OUTCOME ---
            c_out1, c_out2 = st.columns(2)
            res = c_out1.selectbox("Call Result", ["Left Message", "Talked", "Wrong Number"])
            dec = c_out2.selectbox("Decision", ["Pending", "Yes", "No", "Maybe"])
            flag = st.checkbox("🚩 Internal Follow-up Required")

            # --- EMAIL SECTION ---
            st.markdown("### 📧 Follow-up Email")
            send_email_check = st.checkbox(f"Send email to: {new_email}")
            
            final_body = ""
            email_template_name = None
            
            if send_email_check:
                if not templates.empty:
                    email_template_name = st.selectbox("Choose Template", templates['Type'].unique())
                    # Load Body
                    raw_body = templates[templates['Type'] == email_template_name]['Body'].values[0]
                    # Replace Name Logic
                    display_name = new_tp_first if new_tp_first else "Client"
                    final_body = raw_body.replace("{Name}", display_name)
                    # Editor
                    final_body = st.text_area("Edit Message", value=final_body, height=150)
                else:
                    st.warning("No templates found in Google Sheets.")

            # --- SAVE BAR ---
            b1, b2 = st.columns([1, 4])
            
            if b2.button("💾 SAVE CHANGES & FINISH", type="primary", use_container_width=True):
                # 1. Update Details
                df.at[idx, 'Taxpayer First Name'] = new_tp_first
                df.at[idx, 'Taxpayer last name'] = new_tp_last
                df.at[idx, 'Spouse First Name'] = new_sp_first
                df.at[idx, 'Spouse last name'] = new_sp_last
                df.at[idx, 'Home Telephone'] = new_phone
                df.at[idx, 'Taxpayer E-mail Address'] = new_email
                
                # 2. Update CRM
                df.at[idx, 'Status'] = res
                df.at[idx, 'Outcome'] = dec
                df.at[idx, 'Internal_Flag'] = "TRUE" if flag else "FALSE"
                df.at[idx, 'Last_Agent'] = st.session_state.user
                df.at[idx, 'Last_Updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")
                
                # 3. Append Note
                if new_note_input:
                    ts = datetime.now().strftime("%m/%d")
                    df.at[idx, 'Notes'] = current_notes + f"\n[{ts} {st.session_state.user}]: {new_note_input}"

                # 4. Send Email
                if send_email_check and new_email:
                    subject = templates[templates['Type'] == email_template_name]['Subject'].values[0]
                    if send_email_via_gmail(new_email, subject, final_body):
                        st.toast(f"Email sent to {new_email}", icon="📧")
                
                # 5. Save
                with st.spinner("Saving..."):
                    update_data(df, "Clients")
                
                if dec == "Yes": st.balloons()
                st.success("Saved!")
                time.sleep(1)
                st.session_state.current_id = None
                st.rerun()

            if b1.button("Cancel"):
                st.session_state.current_id = None
                st.rerun()

# ==========================================
# 6. ADMIN VIEW
# ==========================================
def render_admin_view():
    st.title("🔒 Admin Dashboard")
    df = get_data("Clients")
    if df.empty: return

    c1, c2, c3 = st.columns(3)
    c1.metric("Total", len(df))
    c2.metric("Pending", len(df[df['Status'] == 'New']))
    c3.metric("YES", len(df[df['Outcome'] == 'Yes']))

    st.markdown("### 📥 Inbox")
    targets = df[(df['Outcome'] == 'Yes') & (df['Status'] != 'Manager Emailed')]
    st.dataframe(targets[['Name', 'Taxpayer E-mail Address', 'Outcome', 'Notes']])

# ==========================================
# 7. ROUTER
# ==========================================
if st.session_state.user is None:
    login_screen()
else:
    if st.session_state.role == "Admin":
        render_admin_view()
    else:
        render_team_view()
