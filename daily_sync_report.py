from __future__ import annotations

import csv
import datetime as dt
import html
import os
import smtplib
import ssl
import sys
from dataclasses import dataclass
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import pandas as pd
import redshift_connector


BASE_DIR = Path(__file__).resolve().parent
HISTORY_PATH = BASE_DIR / "sync_report_history.csv"


def load_local_env(path: Path = BASE_DIR / ".env") -> None:
    """Load local .env values without overriding already exported variables."""
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


load_local_env()


DB_HOST = os.environ.get("REDSHIFT_HOST", "redshift-cluster-1.cagh582pjtts.ap-south-1.redshift.amazonaws.com")
DB_NAME = os.environ.get("REDSHIFT_DATABASE", "dev")
DB_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
DB_USER = os.environ.get("REDSHIFT_USER", "product_analytics")
DB_PASS = os.environ.get("REDSHIFT_PASSWORD") or os.environ.get("REDSHIFT_PASS")

SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "khushal.gogia@gromo.in")
APP_PASSWORD = os.environ.get("GMAIL_APP_PASS")
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL", "khushal.gogia@gromo.in")
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))


@dataclass(frozen=True)
class TableConfig:
    display_name: str
    schema: str
    table: str
    created_column: str = "createdat"
    sync_strategy: str = "created_at"


TABLES: list[TableConfig] = [
    TableConfig("customeraddresses", "gromo_warehouse", "customeraddresses", sync_strategy="row_count"),
    TableConfig("customerprofessions", "gromo_warehouse", "customerprofessions", sync_strategy="row_count"),
    TableConfig("customers", "gromo_warehouse", "customers"),
    TableConfig("productlogs", "gromo_warehouse", "productslogs"),
    TableConfig("userdeviceinfos", "gromo_warehouse", "userdeviceinfos"),
    TableConfig("user_personas", "gromo_warehouse", "user_personas"),
    TableConfig("zohotickets", "gromo_warehouse", "zohotickets"),
    TableConfig("miscellaneousappflyerlogdatas", "gromo_warehouse", "miscellaneousappflyerlogdatas"),
    TableConfig("miscellaneousbranchlogdatas", "gromo_warehouse", "miscellaneousbranchlogdatas"),
    TableConfig("usercompetitorappslists", "gromo_warehouse", "usercompetitorappslist"),
    TableConfig("customerprofiles", "gromo_warehouse", "customerprofiles", sync_strategy="row_count"),
    TableConfig("userproductqualities", "gromo_warehouse", "userproductqualities"),
    TableConfig("brelogsv2", "gromo_warehouse", "brelogsv2"),
    TableConfig(
        "customer-management-system.customerprofiles",
        "gromo_warehouse",
        "customer_management_system_customerprofiles",
        sync_strategy="row_count",
    ),
    TableConfig("leadinfo", "gromo_warehouse", "lead_info"),
    TableConfig("leadpayoutinfo", "gromo_warehouse", "lead_payout_info"),
    TableConfig("users", "gromo_warehouse", "gromo_fintech_users"),
    TableConfig("agencyusers", "gromo_warehouse", "agencyusers"),
    TableConfig("products", "gromo_warehouse", "products"),
]


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def created_at_expression(column_name: str) -> str:
    column = quote_ident(column_name)
    text_value = f"{column}::varchar"
    return f"""
        CASE
            WHEN {column} IS NULL THEN NULL
            WHEN {text_value} LIKE '{{%T%Z%}}'
                THEN REGEXP_REPLACE(
                    REGEXP_SUBSTR({text_value}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[^"}}]+'),
                    'T|Z',
                    ' '
                )::timestamp
            WHEN {text_value} LIKE '{{%'
                THEN TIMESTAMP '1970-01-01 00:00:00'
                    + (CAST(NULLIF(REGEXP_SUBSTR({text_value}, '[0-9]+'), '') AS BIGINT) / 1000)
                    * INTERVAL '1 second'
            WHEN {text_value} ~ '^[0-9]{{12,}}$'
                THEN TIMESTAMP '1970-01-01 00:00:00'
                    + (CAST({text_value} AS BIGINT) / 1000)
                    * INTERVAL '1 second'
            WHEN {text_value} ~ '^-?[0-9]+$'
                THEN NULL
            ELSE {column}::timestamp
        END
    """


def connect_to_redshift():
    if not DB_PASS:
        raise RuntimeError("Missing Redshift password. Set REDSHIFT_PASSWORD in .env.")
    return redshift_connector.connect(
        host=DB_HOST,
        database=DB_NAME,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
    )


def check_one_table(cursor: Any, conn: Any, config: TableConfig) -> dict[str, Any]:
    relation = f"{quote_ident(config.schema)}.{quote_ident(config.table)}"
    column = quote_ident(config.created_column)
    created_at = created_at_expression(config.created_column)
    if config.sync_strategy == "row_count":
        sql = f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT({column}) AS non_null_created_at_count,
                NULL::timestamp AS latest_sync_date
            FROM {relation}
        """
    else:
        sql = f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT({column}) AS non_null_created_at_count,
                MAX({created_at}) AS latest_sync_date
            FROM {relation}
        """

    result = {
        "table_name": config.display_name,
        "physical_table": f"{config.schema}.{config.table}",
        "created_at_column": config.created_column,
        "sync_strategy": config.sync_strategy,
        "row_count": None,
        "non_null_created_at_count": None,
        "latest_sync_date": None,
        "error": "",
    }

    try:
        cursor.execute(sql)
        row_count, non_null_count, latest_sync_date = cursor.fetchone()
        result.update(
            {
                "row_count": row_count,
                "non_null_created_at_count": non_null_count,
                "latest_sync_date": latest_sync_date,
            }
        )
    except Exception as exc:
        conn.rollback()
        result["error"] = str(exc)

    return result


def run_redshift_checks() -> tuple[pd.DataFrame | None, Exception | None]:
    try:
        conn = connect_to_redshift()
    except Exception as exc:
        return None, exc

    try:
        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = 600000;")
            rows = []
            for config in TABLES:
                print(f"Checking {config.display_name} ({config.schema}.{config.table})...")
                rows.append(check_one_table(cursor, conn, config))
            return pd.DataFrame(rows), None
    except Exception as exc:
        return None, exc
    finally:
        conn.close()


def read_history() -> pd.DataFrame:
    if not HISTORY_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(HISTORY_PATH)


def append_history(current_df: pd.DataFrame, run_date: dt.date) -> None:
    fieldnames = [
        "run_date",
        "table_name",
        "physical_table",
        "created_at_column",
        "row_count",
        "non_null_created_at_count",
        "latest_sync_date",
        "error",
    ]
    file_exists = HISTORY_PATH.exists()
    with HISTORY_PATH.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in current_df.to_dict("records"):
            latest = row.get("latest_sync_date")
            writer.writerow(
                {
                    "run_date": run_date.isoformat(),
                    "table_name": row.get("table_name"),
                    "physical_table": row.get("physical_table"),
                    "created_at_column": row.get("created_at_column"),
                    "row_count": row.get("row_count"),
                    "non_null_created_at_count": row.get("non_null_created_at_count"),
                    "latest_sync_date": latest.isoformat() if pd.notna(latest) else "",
                    "error": row.get("error") or "",
                }
            )


def status_from_history(
    history_df: pd.DataFrame,
    table_name: str,
    target_date: dt.date,
    sync_strategy: str,
) -> str:
    if history_df.empty:
        return "Not synced"

    table_history = history_df[history_df["table_name"] == table_name].copy()
    if table_history.empty:
        return "Not synced"

    if sync_strategy == "row_count":
        run_dates = pd.to_datetime(table_history["run_date"], errors="coerce").dt.date
        row_counts = pd.to_numeric(table_history["row_count"], errors="coerce").fillna(0)
        errors = table_history["error"].fillna("").astype(str)
        expected_snapshot_date = target_date + dt.timedelta(days=1)
        usable_history = table_history[
            (run_dates >= expected_snapshot_date) & (row_counts > 0) & (errors == "")
        ].copy()
        return "Synced" if not usable_history.empty else "Not synced"

    latest_dates = pd.to_datetime(table_history["latest_sync_date"], errors="coerce", format="mixed")
    if latest_dates.dropna().empty:
        return "Not synced"

    return "Synced" if latest_dates.max().date() >= target_date else "Not synced"


def build_report_dataframe(current_df: pd.DataFrame, run_date: dt.date) -> pd.DataFrame:
    append_history(current_df, run_date)
    history_df = read_history()
    target_dates = [run_date - dt.timedelta(days=2), run_date - dt.timedelta(days=1)]

    rows = []
    for _, row in current_df.iterrows():
        report_row = {
            "Table": row["table_name"],
            target_dates[0].isoformat(): status_from_history(
                history_df, row["table_name"], target_dates[0], row["sync_strategy"]
            ),
            target_dates[1].isoformat(): status_from_history(
                history_df, row["table_name"], target_dates[1], row["sync_strategy"]
            ),
            "Latest createdat": pd.to_datetime(row["latest_sync_date"], errors="coerce", format="mixed"),
            "Rows": row["row_count"],
            "Note": "Query error" if row.get("error") else ("Zero rows" if row.get("row_count") == 0 else ""),
        }
        rows.append(report_row)

    report_df = pd.DataFrame(rows)
    yesterday_col = target_dates[1].isoformat()
    day_before_col = target_dates[0].isoformat()
    report_df["sort_key"] = report_df[yesterday_col].map({"Not synced": 0, "Synced": 1})
    report_df = report_df.sort_values(["sort_key", day_before_col, "Table"], ascending=[True, True, True])
    return report_df.drop(columns=["sort_key"])


def pill(status: str) -> str:
    colors = {
        "Synced": ("#0f766e", "#d9f5ef"),
        "Not synced": ("#b42318", "#fde8e7"),
    }
    text_color, bg_color = colors.get(status, ("#374151", "#f3f4f6"))
    return (
        f'<span style="display:inline-block;min-width:88px;text-align:center;'
        f'padding:5px 10px;border-radius:999px;font-weight:700;color:{text_color};'
        f'background:{bg_color};">{html.escape(status)}</span>'
    )


def format_latest(value: Any) -> str:
    if pd.isna(value):
        return "-"
    return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M:%S")


def create_report_content(current_df: pd.DataFrame | None, error: Exception | None):
    run_date = dt.date.today()
    target_dates = [run_date - dt.timedelta(days=2), run_date - dt.timedelta(days=1)]

    if error:
        subject = "CRITICAL: Redshift Table Sync Script FAILED"
        text_body = f"The automated table sync report failed.\n\nError details:\n{error}"
        html_body = f"""
        <html><body style="font-family:Arial,sans-serif;">
          <h2>Redshift Table Sync Script Failed</h2>
          <pre style="background:#f8fafc;border:1px solid #e5e7eb;padding:12px;">{html.escape(str(error))}</pre>
        </body></html>
        """
        return subject, text_body, html_body, None

    report_df = build_report_dataframe(current_df, run_date)
    yesterday_col = target_dates[1].isoformat()
    num_not_synced = int((report_df[yesterday_col] == "Not synced").sum())
    total_tables = len(report_df)

    if num_not_synced:
        subject = f"Redshift Sync Report: {num_not_synced} Not synced for {yesterday_col}"
    else:
        subject = f"Redshift Sync Report: All {total_tables} Synced for {yesterday_col}"

    text_body = (
        f"Redshift sync report for {target_dates[0].isoformat()} and {target_dates[1].isoformat()}.\n"
        f"{num_not_synced} of {total_tables} tables are Not synced for {yesterday_col}."
    )

    rows_html = []
    for _, row in report_df.iterrows():
        note = html.escape(str(row["Note"] or ""))
        note_html = f'<span style="color:#b42318;font-weight:600;">{note}</span>' if note else ""
        rows_html.append(
            "<tr>"
            f"<td>{html.escape(str(row['Table']))}</td>"
            f"<td>{pill(str(row[target_dates[0].isoformat()]))}</td>"
            f"<td>{pill(str(row[target_dates[1].isoformat()]))}</td>"
            f"<td>{html.escape(format_latest(row['Latest createdat']))}</td>"
            f"<td style=\"text-align:right;\">{html.escape('' if pd.isna(row['Rows']) else str(int(row['Rows'])))}</td>"
            f"<td>{note_html}</td>"
            "</tr>"
        )

    html_body = f"""
    <html>
      <body style="margin:0;background:#f6f7f9;font-family:Arial,sans-serif;color:#172033;">
        <div style="max-width:1120px;margin:0 auto;padding:28px 20px;">
          <div style="background:#ffffff;border:1px solid #e6e8ee;border-radius:8px;overflow:hidden;">
            <div style="padding:22px 24px;border-bottom:1px solid #e6e8ee;">
              <h2 style="margin:0 0 8px;font-size:22px;">Redshift Sync Report</h2>
              <div style="font-size:14px;color:#586174;">
                {target_dates[0].isoformat()} and {target_dates[1].isoformat()}
              </div>
            </div>
            <div style="padding:18px 24px;display:flex;gap:12px;flex-wrap:wrap;">
              <div style="padding:10px 14px;border:1px solid #e6e8ee;border-radius:8px;">
                <div style="font-size:12px;color:#586174;">Tables</div>
                <div style="font-size:22px;font-weight:800;">{total_tables}</div>
              </div>
              <div style="padding:10px 14px;border:1px solid #e6e8ee;border-radius:8px;">
                <div style="font-size:12px;color:#586174;">Not synced for {yesterday_col}</div>
                <div style="font-size:22px;font-weight:800;color:#b42318;">{num_not_synced}</div>
              </div>
            </div>
            <table style="width:100%;border-collapse:collapse;font-size:13px;">
              <thead>
                <tr style="background:#f8fafc;color:#475467;">
                  <th style="text-align:left;padding:11px 14px;border-top:1px solid #e6e8ee;">Table</th>
                  <th style="text-align:left;padding:11px 14px;border-top:1px solid #e6e8ee;">{target_dates[0].isoformat()}</th>
                  <th style="text-align:left;padding:11px 14px;border-top:1px solid #e6e8ee;">{target_dates[1].isoformat()}</th>
                  <th style="text-align:left;padding:11px 14px;border-top:1px solid #e6e8ee;">Latest createdat</th>
                  <th style="text-align:right;padding:11px 14px;border-top:1px solid #e6e8ee;">Rows</th>
                  <th style="text-align:left;padding:11px 14px;border-top:1px solid #e6e8ee;">Note</th>
                </tr>
              </thead>
              <tbody>
                {"".join(rows_html)}
              </tbody>
            </table>
          </div>
        </div>
      </body>
    </html>
    """

    return subject, text_body, html_body, report_df


def send_email(subject: str, text_body: str, html_body: str, report_df: pd.DataFrame | None) -> None:
    if not APP_PASSWORD:
        raise RuntimeError("Missing Gmail app password. Set GMAIL_APP_PASS before sending email.")

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = SENDER_EMAIL
    message["To"] = RECEIVER_EMAIL
    message.attach(MIMEText(text_body, "plain"))
    message.attach(MIMEText(html_body, "html"))

    if report_df is not None:
        csv_data = report_df.to_csv(index=False)
        attachment = MIMEBase("application", "octet-stream")
        attachment.set_payload(csv_data)
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", "attachment; filename=table_sync_report.csv")
        message.attach(attachment)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, message.as_string())


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    current_df, redshift_error = run_redshift_checks()
    subject, text_body, html_body, report_df = create_report_content(current_df, redshift_error)

    if dry_run:
        print(subject)
        if report_df is not None:
            print(report_df.to_string(index=False))
        else:
            print(text_body)
        return 0 if redshift_error is None else 1

    send_email(subject, text_body, html_body, report_df)
    print(f"Email successfully sent to {RECEIVER_EMAIL}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
