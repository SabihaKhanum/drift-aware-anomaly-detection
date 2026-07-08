import psycopg2, os
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

cur.execute("""
    ALTER TABLE anomaly_alerts 
    ADD COLUMN IF NOT EXISTS adaptive_mode BOOLEAN,
    ADD COLUMN IF NOT EXISTS crisis_mode BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS severity_context TEXT;
""")

cur.execute("""
    ALTER TABLE drift_events
    ADD COLUMN IF NOT EXISTS severity TEXT,
    ADD COLUMN IF NOT EXISTS adaptation_strategy TEXT,
    ADD COLUMN IF NOT EXISTS vol_ratio DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS z_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS drift_rate DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS adwin_width INT,
    ADD COLUMN IF NOT EXISTS crisis_mode BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS confirmation_count INT,
    ADD COLUMN IF NOT EXISTS first_signal_time TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS adaptation_latency_s DOUBLE PRECISION;
""")

conn.commit()
print("Schema updated successfully")
conn.close()