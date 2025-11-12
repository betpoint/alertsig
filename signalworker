import psycopg2
import requests
import schedule
import time
import logging
import os

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL connection details (read from environment)
conn_details = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

# OneSignal API details (read from environment)
ONE_SIGNAL_API_URL = "https://onesignal.com/api/v1/notifications"
ONE_SIGNAL_APP_ID = os.getenv("ONE_SIGNAL_APP_ID")
ONE_SIGNAL_API_KEY = os.getenv("ONE_SIGNAL_API_KEY")

# Keep track of IDs already notified
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
            logging.info(f"Notification sent successfully: {message}")
        else:
            logging.error(f"Failed to send notification. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error sending notification: {e}")

def check_new_rows():
    logging.info("Checking for new rows in signal_main...")
    try:
        conn = psycopg2.connect(**conn_details)
        cursor = conn.cursor()

        query = """
        SELECT id, home_team, away_team, refer_team, league, final_average, signal,
               n1_count, n2_count, n3_count, odds, home_q1, home_q2, home_q3, away_q1, away_q2, away_q3,
               diff, total, home_score, away_score, date, date_time, link 
        FROM signal_main
        WHERE odds < 20
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            row_id = row[0]
            if row_id not in notified_ids:
                notified_ids.add(row_id)
                message = f"New row added: ID {row_id}, Home: {row[1]}, Away: {row[2]}, League: {row[4]}."
                send_notification(message)
                logging.info(f"New row detected and notified: ID {row_id}")

    except Exception as e:
        logging.error(f"Error checking new rows: {e}")

# Run the task every 10 seconds
schedule.every(10).seconds.do(check_new_rows)

logging.info("Starting the polling for new rows in signal_main...")

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Script stopped manually.")
        break
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        time.sleep(5)

