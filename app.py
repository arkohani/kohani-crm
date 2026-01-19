
import streamlit as st
import pandas as pd
import gspread
@@ -44,6 +43,7 @@
    .email-date { font-size: 0.8em; color: #666; }
    .email-subject { font-weight: bold; color: #004B87; }
    .email-snippet { font-size: 0.9em; color: #333; }
    .warning-box { background-color: #fff3cd; color: #856404; padding: 10px; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

@@ -52,7 +52,7 @@

SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly', # Added for searching inbox
    'https://www.googleapis.com/auth/gmail.readonly', # Required for reading history
    'https://www.googleapis.com/auth/gmail.settings.basic',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
@@ -127,15 +127,15 @@ def get_user_signature():

def search_gmail_messages(query_emails):
    """
    Searches the logged-in user's Gmail for emails to/from the provided list.
    Returns a list of dicts: {id, threadId, subject, snippet, date}
    Searches the logged-in user's Gmail.
    Returns: (list_of_emails, error_message)
    """
    if not query_emails: return []
    if not query_emails: return [], "No email addresses to search."
    try:
        service = get_gmail_service()
        # Construct query: from:a@b.com OR to:a@b.com
        q_parts = [f"from:{e} OR to:{e}" for e in query_emails if e and "@" in e]
        if not q_parts: return []
        if not q_parts: return [], "Invalid email format."

        full_query = " OR ".join(q_parts)

@@ -157,11 +157,12 @@ def search_gmail_messages(query_emails):
                    'date': date_str,
                    'snippet': snippet
                })
        return email_data
        return email_data, None
    except Exception as e:
        # Fail silently or return empty if permission denied/error
        # print(e) 
        return []
        error_str = str(e)
        if "403" in error_str or "insufficient" in error_str.lower():
            return [], "PERM_ERROR"
        return [], f"Gmail API Error: {error_str}"

def send_email_as_user(to_email, subject, body_text, body_html):
    try:
@@ -195,7 +196,6 @@ def send_email_as_user(to_email, subject, body_text, body_html):
# 3. DATABASE FUNCTIONS & HELPERS
# ==========================================
def normalize_phone(phone):
    """Strips everything except digits to allow loose matching."""
    if not phone: return ""
    return re.sub(r'\D', '', str(phone))

@@ -221,6 +221,7 @@ def get_data(worksheet_name="Clients"):
            ws = sh.worksheet(worksheet_name)
        except:
            if worksheet_name == "Templates": return pd.DataFrame(columns=['Type', 'Subject', 'Body'])
            # If Reference is missing, return empty but don't crash
            if worksheet_name == "Reference": return pd.DataFrame()
            return pd.DataFrame()

@@ -239,13 +240,22 @@ def get_data(worksheet_name="Clients"):
        df = pd.DataFrame(rows, columns=unique_headers)

        if worksheet_name == "Clients":
            # Map standard columns if they don't exist exactly
            if 'Notes' not in df.columns:
                # Try to find a history column
                for c in df.columns:
                    if 'history' in c.lower() or 'note' in c.lower():
                        df.rename(columns={c: 'Notes'}, inplace=True)
                        break
            
            required_cols = ['Status', 'Outcome', 'Internal_Flag', 'Notes', 'Last_Agent', 'Last_Updated', 'Gender', 'Spouse E-mail Address']
            for col in required_cols:
                if col not in df.columns: df[col] = ""
            df['Status'] = df['Status'].replace("", "New")

        return df
    except Exception as e:
        st.error(f"DB Error ({worksheet_name}): {e}")
        return pd.DataFrame()

def update_data(df, worksheet_name="Clients"):
@@ -330,11 +340,13 @@ def render_client_card_editor(df, df_ref, templates, client_id):
    client = df.loc[idx]

    # ------------------------------------
    # MANUAL REFERENCE SEARCH UI (Feature #3)
    # MANUAL REFERENCE SEARCH UI (Fixed)
    # ------------------------------------
    if not df_ref.empty:
        with st.sidebar.expander("🔎 Manual Reference Search", expanded=False):
            st.caption("Check the Reference sheet for contact info.")
    with st.sidebar.expander("🔎 Manual Reference Search", expanded=True):
        if df_ref.empty:
            st.warning("⚠️ Reference sheet is empty or not found. Please check tab name 'Reference'.")
        else:
            st.caption(f"Searching {len(df_ref)} rows in 'Reference'...")
            ref_search = st.text_input("Type name or phone:", key="manual_ref_search")
            if ref_search:
                # Basic search across all columns in Ref
@@ -343,11 +355,10 @@ def render_client_card_editor(df, df_ref, templates, client_id):
                if not ref_hits.empty:
                    st.write(f"Found {len(ref_hits)} matches:")
                    for _, r_row in ref_hits.iterrows():
                        # Try to display something useful
                        disp_str = " | ".join([str(val) for val in r_row.values if str(val)])
                        st.text_area("Match", disp_str[:200], height=60)
                        st.text_area("Match", disp_str[:200], height=80)
                else:
                    st.warning("No matches in Reference sheet.")
                    st.warning("No matches found.")

    with st.container(border=True):
        # Header
@@ -373,13 +384,11 @@ def render_client_card_editor(df, df_ref, templates, client_id):

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
@@ -433,11 +442,11 @@ def check_token_match(ref_val):
        tab_notes, tab_email, tab_gmail_hist = st.tabs(["📝 Notes / History", "✉️ Compose Email", "📧 Gmail History"])

        with tab_notes:
            st.text_area("History Log", str(client.get('Notes', '')), disabled=True, height=150)
            st.text_area("History Log", str(client.get('Notes', '')), disabled=True, height=200)
            new_note = st.text_area("Add Note")

        with tab_gmail_hist:
            # Feature #4: Unified-ish Inbox (Search logged-in user's gmail)
            # Feature #4: Unified-ish Inbox with Error Handling
            st.caption("Searching your Gmail for correspondence with this client...")
            search_targets = []
            if new_tp_email: search_targets.append(new_tp_email)
@@ -446,8 +455,14 @@ def check_token_match(ref_val):
            if not search_targets:
                st.info("No email addresses on file to search.")
            else:
                gmail_results = search_gmail_messages(search_targets)
                if gmail_results:
                gmail_results, error_msg = search_gmail_messages(search_targets)
                if error_msg:
                    if error_msg == "PERM_ERROR":
                        st.error("⚠️ Access Denied: We cannot read your emails yet.")
                        st.markdown("**Action Required:** Please click 'Logout' in the sidebar and Sign In again to grant the new permissions.")
                    else:
                        st.error(error_msg)
                elif gmail_results:
                    for msg in gmail_results:
                        st.markdown(f"""
                        <div class="email-row">
@@ -460,7 +475,7 @@ def check_token_match(ref_val):
                    st.write("No emails found in your inbox for these addresses.")

        with tab_email:
            st.caption("Feature: Emails sent here are automatically logged to Notes.")
            st.caption("Emails sent here are automatically logged to Notes.")
            email_targets = {}
            if new_tp_email: email_targets[f"Taxpayer: {new_tp_email}"] = new_tp_email
            if new_sp_email: email_targets[f"Spouse: {new_sp_email}"] = new_sp_email
@@ -514,14 +529,9 @@ def check_token_match(ref_val):
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
        stat_idx = status_opts.index(curr_stat) if curr_stat in status_opts else 0

        res = c_out1.selectbox("Call Result", status_opts, index=stat_idx)

@@ -541,7 +551,7 @@ def check_token_match(ref_val):
            st.rerun()

        if col_b2.button("💾 SAVE & FINISH", type="primary", use_container_width=True):
            # Update Data
            # Prepare Update
            df.at[idx, 'Taxpayer First Name'] = new_tp_first
            df.at[idx, 'Spouse First Name'] = new_sp_first
            df.at[idx, 'Taxpayer last name'] = new_tp_last
@@ -556,26 +566,32 @@ def check_token_match(ref_val):
            df.at[idx, 'Last_Agent'] = st.session_state.user_email
            df.at[idx, 'Last_Updated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

            # Combine notes
            # Combine notes & Email Log
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            note_append = ""
            
            # 1. Add User Note
            if new_note:
                note_append += f"\n[{timestamp} {st.session_state.user_email}]: {new_note}"

            # 2. Process Email
            if enable_email and selected_email_address:
                if send_email_as_user(selected_email_address, subj, final_text, final_html):
                    st.toast(f"Email sent to {selected_email_address}")
                    # Feature #4 (Option B): Log Email to Notes
                    note_append += f"\n[EMAIL SENT by {st.session_state.user_email}]\nSubject: {subj}\nTo: {selected_email_address}\n"
                    # Log Email to Notes
                    note_append += f"\n----------------------------------------\n[📧 EMAIL SENT] {timestamp}\nTo: {selected_email_address}\nSubject: {subj}\n----------------------------------------"
                else:
                    st.error("Failed to send email.")

            if note_append:
                df.at[idx, 'Notes'] = str(client.get('Notes', '')) + note_append
                current_notes = str(client.get('Notes', ''))
                df.at[idx, 'Notes'] = current_notes + note_append

            with st.spinner("Saving..."):
            with st.spinner("Saving to Google Sheets..."):
                update_data(df, "Clients")

            if dec == "Yes": st.balloons()
            st.success("Saved!")
            st.success("Saved Successfully!")
            time.sleep(1)
            st.session_state.current_id = None
            st.session_state.admin_current_id = None
@@ -594,14 +610,12 @@ def render_team_view(df, df_ref, templates, user_email):
            with st.container(border=True):
                st.write("### 📞 Call Queue")

                # Feature #2: Ensure we only pick explicitly 'New' statuses to avoid recalling people
                # Exclude worked statuses
                queue = df[df['Status'] == 'New']
                st.metric("New Leads Remaining", len(queue))

                if not queue.empty:
                    if st.button("🎲 START CALL (Prioritize Phones)", type="primary", use_container_width=True):
                        # Feature #1: Prioritize Phone Numbers
                        # Check if phone has at least 7 digits
                        queue['clean_phone'] = queue['Home Telephone'].apply(normalize_phone)
                        with_phone = queue[queue['clean_phone'].str.len() > 6]
                        no_phone = queue[queue['clean_phone'].str.len() <= 6]
@@ -621,16 +635,9 @@ def render_team_view(df, df_ref, templates, user_email):
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
@@ -813,8 +820,8 @@ def render_admin_view(df, df_ref, templates, user_email):
                                        # Log to notes
                                        existing_notes = str(df.at[idx, 'Notes'])
                                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                        log_entry = f"[{timestamp} {st.session_state.user_email}]: {new_note}\n[EMAIL SENT] To: {selected_email_addr}"
                                        df.at[idx, 'Notes'] = f"{existing_notes}\n{log_entry}"
                                        log_entry = f"\n[{timestamp} {st.session_state.user_email}]: {new_note}\n----------------\n[📧 MANAGER EMAIL SENT] {timestamp}\nTo: {selected_email_addr}"
                                        df.at[idx, 'Notes'] = f"{existing_notes}{log_entry}"

                                        update_data(df, "Clients")
