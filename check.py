
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"), port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"), password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB"),
)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM anomaly_alerts;")
print("Total ticks logged:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM drift_events;")
print("Drift events:", cur.fetchone()[0])

cur.execute("""
    SELECT detector, 
           ROUND(AVG(score)::numeric, 4) as avg_score,
           ROUND(MIN(score)::numeric, 4) as min_score,
           ROUND(MAX(score)::numeric, 4) as max_score,
           COUNT(*) FILTER (WHERE is_anomaly) as anomalies,
           COUNT(*) as total
    FROM anomaly_alerts
    GROUP BY detector ORDER BY detector;
""")
for row in cur.fetchall():
    print(row)

conn.close()