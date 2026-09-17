
# import psycopg2, os
# from dotenv import load_dotenv
# load_dotenv()

# conn = psycopg2.connect(
#     host=os.getenv("TIMESCALE_HOST"), port=os.getenv("TIMESCALE_PORT"),
#     user=os.getenv("TIMESCALE_USER"), password=os.getenv("TIMESCALE_PASSWORD"),
#     dbname=os.getenv("TIMESCALE_DB"),
# )
# cur = conn.cursor()

# cur.execute("SELECT COUNT(*) FROM anomaly_alerts;")
# print("Total ticks logged:", cur.fetchone()[0])

# cur.execute("SELECT COUNT(*) FROM drift_events;")
# print("Drift events:", cur.fetchone()[0])

# cur.execute("""
#     SELECT detector, 
#            ROUND(AVG(score)::numeric, 4) as avg_score,
#            ROUND(MIN(score)::numeric, 4) as min_score,
#            ROUND(MAX(score)::numeric, 4) as max_score,
#            COUNT(*) FILTER (WHERE is_anomaly) as anomalies,
#            COUNT(*) as total
#     FROM anomaly_alerts
#     GROUP BY detector ORDER BY detector;
# """)
# for row in cur.fetchall():
#     print(row)


# =================================================
import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"),
    port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"),
    password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB"),
)
cur = conn.cursor()
cur.execute("TRUNCATE anomaly_alerts;")
cur.execute("TRUNCATE drift_events;")
conn.commit()
print("Tables truncated")
conn.close()

# import os
# from dotenv import load_dotenv

# load_dotenv()
# csv_path = os.getenv("BACKTEST_CSV_PATH")
# print(f"Path: {csv_path}")
# print(f"Exists: {os.path.exists(csv_path) if csv_path else 'None'}")