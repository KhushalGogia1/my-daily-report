import redshift_connector
import pandas as pd
import sys
from textwrap import dedent
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import datetime
import os  # <-- THIS IS NEW

# --- 1. SCRIPT CONFIGURATION (Passwords removed) ---

# Redshift Credentials
DB_HOST = 'redshift-cluster-1.cagh582pjtts.ap-south-1.redshift.amazonaws.com'
DB_NAME = 'dev'
DB_PORT = 5439
DB_USER = 'product_analytics'
DB_PASS = os.environ.get('REDSHIFT_PASS') # <-- THIS IS THE PLACEHOLDER

# Email Credentials
SENDER_EMAIL = "khushal.gogia@gromo.in"
APP_PASSWORD = os.environ.get('GMAIL_APP_PASS') # <-- THIS IS THE PLACEHOLDER
RECEIVER_EMAIL = "khushal.gogia@gromo.in"

# Email Server
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465

# Report Settings
# With a value of 1, today and yesterday are OK.
# Any date before yesterday will be flagged.
SYNC_THRESHOLD_DAYS = 1


# --- 2. REDSHIFT QUERY FUNCTIONS (No changes here) ---

def get_queries_to_run():
    """
    Defines all queries to be combined.
    """
    
    # Robust epoch_query
    def epoch_query(table_name):
        query_template = dedent(f"""
            SELECT max(TIMESTAMP '1970-01-01 00:00:00' + (CAST(NULLIF(REGEXP_SUBSTR(createdat, '[0-9]+'), '') AS BIGINT) / 1000) * INTERVAL '1 second')
            FROM gromo_warehouse.{table_name}
        """)
        return query_template

    queries_to_run = {
        # --- Tables using epoch/string conversion ---
        "mi_four_wheeler_royal_sundaram_kyc_status_apilog": epoch_query("mi_four_wheeler_royal_sundaram_kyc_status_apilog"),
        "mi_four_wheeler_united_search_kyc_data_apilog": epoch_query("mi_four_wheeler_united_search_kyc_data_apilog"),
        "mi_four_wheeler_digit_kyc_status_apilog": epoch_query('"mi_four_wheeler_digit_kyc_status_apilog"'),
        "mi_four_wheeler_icici_kyc_apilogs": epoch_query('"mi_four_wheeler_icici_kyc_apilogs"'),
        "mi_four_wheeler_reliance_kyc_apilogs": epoch_query('"mi_four_wheeler_reliance_kyc_apilogs"'),
        "mi_four_wheeler_tata_kyc_apilog": epoch_query('"mi_four_wheeler_tata_kyc_apilog"'),
        "customers": epoch_query('"customers"'),
        "user_personas": epoch_query('"user_personas"'),
        "zohotickets": epoch_query('"zohotickets"'),
        "posmodels": epoch_query('"posmodels"'),
        "userexammodels": epoch_query('"userexammodels"'),
        "insuranceleads": epoch_query('"insuranceleads"'),
        "educationmodels": epoch_query('"educationmodels"'),
        "panmodels": epoch_query('"panmodels"'),
        "selfiemodels": epoch_query('"selfiemodels"'),
        "mi_four_wheeler_bajaj_kyc_search_apilog": epoch_query('"mi_four_wheeler_bajaj_kyc_search_apilog"'),
        "mi_four_wheeler_reliance_quote_apilogs": epoch_query("mi_four_wheeler_reliance_quote_apilogs"),
        "mi_four_wheeler_sbi_proposal_apilogs": epoch_query("mi_four_wheeler_sbi_proposal_apilogs"),
        "mi_four_wheeler_digit_quote_apilogs": epoch_query("mi_four_wheeler_digit_quote_apilogs"),
        "mi_four_wheeler_reliance_proposal_apilogs": epoch_query("mi_four_wheeler_reliance_proposal_apilogs"),
        "mi_four_wheeler_icici_quote_apilogs": epoch_query("mi_four_wheeler_icici_quote_apilogs"),
        "mi_four_wheeler_icici_proposal_apilogs": epoch_query("mi_four_wheeler_icici_proposal_apilogs"),
        "mi_four_wheeler_digit_proposal_apilogs": epoch_query("mi_four_wheeler_digit_proposal_apilogs"),
        "mi_four_wheeler_bajaj_proposal_apilogs": epoch_query("mi_four_wheeler_bajaj_proposal_apilogs"),
        "mi_four_wheeler_sbi_quote_apilogs": epoch_query("mi_four_wheeler_sbi_quote_apilogs"),
        "mi_four_wheeler_bajaj_quote_apilogs": epoch_query('"mi_four_wheeler_bajaj_quote_apilogs"'),
        "mi_four_wheeler_tata_proposal_apilogs": epoch_query("mi_four_wheeler_tata_proposal_apilogs"),
        "mi_four_wheeler_tata_quote_apilogs": epoch_query("mi_four_wheeler_tata_quote_apilogs"),
        "mi_four_wheeler_united_quote_apilogs": epoch_query("mi_four_wheeler_united_quote_apilogs"),
        "mi_four_wheeler_united_proposal_apilogs": epoch_query("mi_four_wheeler_united_proposal_apilogs"),
        "mi_four_wheeler_hdfc_quote_apilogs": epoch_query("mi_four_wheeler_hdfc_quote_apilogs"),
        "mi_four_wheeler_hdfc_proposal_apilogs": epoch_query("mi_four_wheeler_hdfc_proposal_apilogs"),
        "mi_four_wheeler_royal_sundaram_proposal_apilogs": epoch_query("mi_four_wheeler_royal_sundaram_proposal_apilogs"),
        "mi_four_wheeler_royal_sundaram_quote_apilogs": epoch_query("mi_four_wheeler_royal_sundaram_quote_apilogs"),
        "mi_four_wheeler_internal_apilogs": epoch_query("mi_four_wheeler_internal_apilogs"),
        "mi_two_wheeler_reliance_quote_apilogs": epoch_query("mi_two_wheeler_reliance_quote_apilogs"),
        "mi_two_wheeler_sbi_proposal_apilogs": epoch_query("mi_two_wheeler_sbi_proposal_apilogs"),
        "mi_two_wheeler_digit_quote_apilogs": epoch_query("mi_two_wheeler_digit_quote_apilogs"),
        "mi_two_wheeler_reliance_proposal_apilogs": epoch_query("mi_two_wheeler_reliance_proposal_apilogs"),
        "mi_two_wheeler_icici_quote_apilogs": epoch_query("mi_two_wheeler_icici_quote_apilogs"),
        "mi_two_wheeler_icici_proposal_apilogs": epoch_query("mi_two_wheeler_icici_proposal_apilogs"),
        "mi_two_wheeler_digit_proposal_apilogs": epoch_query("mi_two_wheeler_digit_proposal_apilogs"),
        "mi_two_wheeler_bajaj_proposal_apilogs": epoch_query("mi_two_wheeler_bajaj_proposal_apilogs"),
        "mi_two_wheeler_sbi_quote_apilogs": epoch_query("mi_two_wheeler_sbi_quote_apilogs"),
        "mi_two_wheeler_bajaj_quote_apilogs": epoch_query("mi_two_wheeler_bajaj_quote_apilogs"),
        "mi_two_wheeler_tata_proposal_apilogs": epoch_query("mi_two_wheeler_tata_proposal_apilogs"),
        "mi_two_wheeler_tata_quote_apilogs": epoch_query("mi_two_wheeler_tata_quote_apilogs"),
        "miscellaneousbranchlogdatas": epoch_query("miscellaneousbranchlogdatas"),
        "miscellaneousappflyerlogdatas": epoch_query("miscellaneousappflyerlogdatas"),
        "usercompetitorappslist": epoch_query("usercompetitorappslist"),
        "userproductqualities": epoch_query("userproductqualities"),

        # Robust special queries
        "gromo_fintech_users": """
            SELECT max(
              (CASE
                 WHEN createdat LIKE '{%' THEN json_extract_path_text(createdat, '$date')
                 ELSE createdat::text
               END
              )::timestamp
            )
            FROM gromo_warehouse."gromo_fintech_users"
        """,
        "overall_gp_lead_info (gp_created_date)": """
            SELECT max(
              (CASE
                 WHEN gp_created_date LIKE '{%' THEN json_extract_path_text(gp_created_date, '$date')
                 ELSE gp_created_date::text
               END
              )::timestamp
            )
            FROM gromo_warehouse.overall_gp_lead_info
        """,
        "overall_gp_lead_info (leads_created_at)": """
            SELECT max(
              (CASE
                 WHEN leads_created_at LIKE '{%' THEN json_extract_path_text(leads_created_at, '$date')
                 ELSE leads_created_at::text
               END
              )::timestamp
            )
            FROM gromo_warehouse.overall_gp_lead_info
        """
    }
    return queries_to_run

def build_master_query(queries_dict):
    """Combines all individual queries into one master UNION ALL query."""
    query_parts = []
    for table_alias, sub_query in queries_dict.items():
        safe_alias = table_alias.replace("'", "''")
        part = dedent(f"""
            SELECT '{safe_alias}' as table_name, latest_sync_date
            FROM (
                {sub_query}
            ) as t(latest_sync_date)
        """)
        query_parts.append(part)
    master_sql = " \nUNION ALL\n ".join(query_parts)
    return master_sql

def run_redshift_query():
    """
    Connects to Redshift, runs the master query, and returns a DataFrame.
    Returns (df, None) on success, or (None, error) on failure.
    """
    conn = None
    try:
        conn = redshift_connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS  # This now uses the placeholder
        )
        print('✅ Connection successful.')
        queries_dict = get_queries_to_run()
        print(f"Checking sync status for {len(queries_dict)} tables...")
        master_query = build_master_query(queries_dict)
        
        print("Setting session timeout to 10 minutes...")
        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = 600000;")
        print("Timeout set.")
        
        print("Executing master query... This may take a few minutes.")
        df = pd.read_sql_query(master_query, conn)
        print("✅ Successfully fetched all sync dates.")
        return df, None

    except Exception as e:
        print(f"❌ A Redshift error occurred: {e}", file=sys.stderr)
        return None, e # Return the error
    
    finally:
        if conn:
            conn.close()
            print("Connection closed.")


# --- 3. REPORTING AND EMAIL FUNCTIONS (No changes here) ---

def create_report_content(df, error):
    """
    Analyzes the DataFrame or error and generates the email subject,
    body, and HTML.
    """
    
    # Get today's date
    # .normalize() sets the time to 00:00:00
    today = pd.to_datetime('today').normalize()
    
    if error:
        # --- FAILURE CASE ---
        subject = "❌ CRITICAL: Redshift Table Sync Script FAILED"
        text_body = f"""
        Hello,

        The automated table sync report script failed to run.
        It could not connect to Redshift or the query failed.

        Error details:
        {error}
        """
        html_body = f"""
        <html><body>
        <h2>❌ CRITICAL: Redshift Table Sync Script FAILED</h2>
        <p>The automated table sync report script failed to run.
        It could not connect to Redshift or the query failed.</p>
        <h3>Error details:</h3>
        <pre style="background-color:#f1f1f1; border:1px solid #ddd; padding:10px;">
        {error}
        </pre>
        </body></html>
        """
        return subject, text_body, html_body, None # No DataFrame to attach

    # --- SUCCESS CASE ---
    
    # Define the cutoff date for "Not Synced"
    cutoff_date = today - pd.Timedelta(days=SYNC_THRESHOLD_DAYS)
    
    # Create the report DataFrame
    report_df = df.copy()
    
    # Ensure the date column is a proper datetime object
    report_df['latest_sync_date'] = pd.to_datetime(report_df['latest_sync_date'])

    # 1. Create the 'Status' column
    report_df['Status'] = '✅ Synced'
    report_df.loc[report_df['latest_sync_date'].dt.date < cutoff_date.date(), 'Status'] = '❌ NOT SYNCED'

    # 2. Sort the report to show "NOT SYNCED" tables at the top
    report_df = report_df.sort_values(by=['Status', 'latest_sync_date'], ascending=[False, True])
    
    # 3. Get summary numbers
    total_tables = len(report_df)
    not_synced_tables = report_df[report_df['Status'] == '❌ NOT SYNCED']
    num_not_synced = len(not_synced_tables)

    # 4. Create dynamic Subject and Body
    if num_not_synced == 0:
        subject = f"✅ Redshift Sync Report: All {total_tables} Tables OK"
        text_summary = f"Great news! All {total_tables} tables are synced as of {today.strftime('%Y-%m-%d')}."
    else:
        subject = f"⚠️ Redshift Sync Report: {num_not_synced} Tables NOT SYNCED"
        text_summary = f"Warning! {num_not_synced} out of {total_tables} tables are NOT SYNCED."
    
    text_body = f"""
    Hello,

    {text_summary}
    (Sync threshold: Flagging tables not updated since {cutoff_date.strftime('%Y-%m-%d')})

    Please find the full report below.
    This is an automated report.

    Best regards,
    Python Bot
    """

    # 5. Create HTML body with styled table
    def style_row(row):
        if row.Status == '❌ NOT SYNCED':
            return ['background-color: #ffe6e6'] * len(row) # Light red
        return [''] * len(row)

    # Apply the styling and convert to HTML
    html_table = (
        report_df.style
        .apply(style_row, axis=1)
        .format({'latest_sync_date': "{:%Y-%m-%d %H:%M:%S}"})
        .hide(axis="index")
        .to_html()
    )

    html_body = f"""
    <html>
      <head>
        <style>
          body {{ font-family: 'Arial', sans-serif; }}
          table {{ border-collapse: collapse; margin-top: 20px; }}
          th, td {{ 
            border: 1px solid #ddd; 
            padding: 8px; 
            text-align: left; 
          }}
          th {{ background-color: #f2f2f2; }}
        </style>
      </head>
      <body>
        <h2>Redshift Table Sync Report</h2>
        <p><b>{text_summary}</b></p>
        <p>(Sync threshold: Tables with last update before {cutoff_date.strftime('%Y-%m-%d')} are flagged)</p>
        
        <h3>Full Report (Not Synced at top)</h3>
        {html_table}
      </body>
    </html>
    """
    
    return subject, text_body, html_body, report_df


def send_email(subject, text_body, html_body, report_df):
    """
    Connects to the SMTP server and sends the composed email.
    """
    
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = SENDER_EMAIL
    message["To"] = RECEIVER_EMAIL

    # Attach text and HTML parts
    message.attach(MIMEText(text_body, "plain"))
    message.attach(MIMEText(html_body, "html"))

    # Conditionally attach CSV
    if report_df is not None:
        try:
            csv_data = report_df.to_csv(index=False)
            part3 = MIMEBase("application", "octet-stream")
            part3.set_payload(csv_data)
            encoders.encode_base64(part3)
            part3.add_header(
                "Content-Disposition",
                "attachment; filename=table_sync_report.csv",
            )
            message.attach(part3)
            print("CSV attachment added.")
        except Exception as e:
            print(f"Warning: Could not create CSV attachment. {e}")

    # Send the email
    print(f"Connecting to email server {SMTP_SERVER}...")
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, APP_PASSWORD) # This now uses the placeholder
            print("Login successful.")
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, message.as_string())
            print(f"Email successfully sent to {RECEIVER_EMAIL}!")

    except smtplib.SMTPAuthenticationError:
        print("Error: Authentication failed. Check your SENDER_EMAIL and APP_PASSWORD.", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)


# --- 4. MAIN EXECUTION (No changes here) ---

if __name__ == "__main__":
    # 1. Get the data (or an error)
    df, redshift_error = run_redshift_query()
    
    # 2. Create the email content based on success or failure
    subject, text_body, html_body, report_df = create_report_content(df, redshift_error)
    
    # 3. Send the email
    send_email(subject, text_body, html_body, report_df)