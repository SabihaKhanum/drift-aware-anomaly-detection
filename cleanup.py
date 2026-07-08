# Run cleanup2.py
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"), port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"), password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB"),
)
cur = conn.cursor()
cur.execute("DELETE FROM anomaly_alerts;")
cur.execute("DELETE FROM drift_events;")
conn.commit()
print("All data cleared")
conn.close()