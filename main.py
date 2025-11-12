import psycopg2
import requests
import schedule
import time
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL connection details
conn_details = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

# OneSignal API details
ONE_SIGNAL_API_URL = "https://onesignal.com/api/v1/notifications"
ONE_SIGNAL_APP_ID = os.getenv("ONE_SIGNAL_APP_ID")
ONE_SIGNAL_API_KEY = os.getenv("ONE_SIGNAL_API_KEY")

notified_ids = set()

def send_notification(message):
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Basic {ONE_SIGNAL_API_KEY}"
    }
    payload = {
        "app_id": ONE_SIGNAL_APP_ID,
        "included_segments": ["All"],
        "headings": {"en": "New Data Alert"},
        "contents": {"en": message}
    }
    try:
        response = requests.post(ONE_SIGNAL_API_URL, headers=headers, json=payload)
        if response.status_code == 200:
            logging.info(f"✅ Notification sent: {message}")
        else:
            logging.error(f"❌ Failed to send: {response.status_code} {response.text}")
    except Exception as e:
        logging.error(f"❌ Error sending notification: {e}")

def check_new_rows():
    logging.info("🔍 Checking for new rows...")
    try:
        conn = psycopg2.connect(**conn_details)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, home_team, away_team, league
            FROM signal_main
            WHERE odds < 20
            ORDER BY date_time DESC
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            row_id = row[0]
            if row_id not in notified_ids:
                notified_ids.add(row_id)
                message = f"New Signal: {row[1]} vs {row[2]} ({row[3]})"
                send_notification(message)
                logging.info(f"📣 Notified ID {row_id}")

    except Exception as e:
        logging.error(f"❌ Database check error: {e}")

schedule.every(10).seconds.do(check_new_rows)

logging.info("🚀 Worker started...")
while True:
    schedule.run_pending()
    time.sleep(1)

