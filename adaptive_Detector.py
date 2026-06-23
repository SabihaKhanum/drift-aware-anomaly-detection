import os
import json
import numpy as np
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg2
from river import drift, anomaly
from sklearn.ensemble import IsolationForest
from collections import deque

load_dotenv()

# ─── Database connection ───────────────────────────────
conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"),
    port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"),
    password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB"),
)
cur = conn.cursor()

# ─── Create tables if not exist ───────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS drift_events (
    time        TIMESTAMPTZ NOT NULL,
    detector    TEXT,
    price       DOUBLE PRECISION,
    message     TEXT
);
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT,
    price       DOUBLE PRECISION,
    detector    TEXT,
    score       DOUBLE PRECISION,
    is_anomaly  BOOLEAN
);
""")
conn.commit()

# ─── Drift detectors ──────────────────────────────────
adwin = drift.ADWIN()
page_hinkley = drift.PageHinkley()

# ─── Anomaly detectors ────────────────────────────────
# CUSUM — manual implementation
class CUSUM:
    def __init__(self, threshold=5.0, drift_param=0.5):
        self.threshold = threshold
        self.drift_param = drift_param
        self.mean = None
        self.cusum_pos = 0
        self.cusum_neg = 0
        self.history = deque(maxlen=100)

    def update(self, value):
        self.history.append(value)
        if self.mean is None:
            self.mean = value
            return False, 0
        deviation = value - self.mean - self.drift_param
        self.cusum_pos = max(0, self.cusum_pos + deviation)
        self.cusum_neg = max(0, self.cusum_neg - deviation - self.drift_param)
        is_anomaly = self.cusum_pos > self.threshold or self.cusum_neg > self.threshold
        # rolling mean update
        self.mean = np.mean(self.history)
        score = max(self.cusum_pos, self.cusum_neg)
        return is_anomaly, score

    def recalibrate(self):
        if len(self.history) > 10:
            self.mean = np.mean(self.history)
            self.cusum_pos = 0
            self.cusum_neg = 0
            print("CUSUM recalibrated")

# Isolation Forest — retrain on recent window
class AdaptiveIsolationForest:
    def __init__(self, window_size=200):
        self.window = deque(maxlen=window_size)
        self.model = None
        self.trained = False

    def update(self, value):
        self.window.append([value])
        if len(self.window) >= 50 and not self.trained:
            self.retrain()
        if not self.trained:
            return False, 0
        score = self.model.score_samples([[value]])[0]
        is_anomaly = score < -0.5
        return is_anomaly, abs(score)

    def retrain(self):
        if len(self.window) >= 50:
            self.model = IsolationForest(contamination=0.05, random_state=42)
            self.model.fit(list(self.window))
            self.trained = True
            print(f"Isolation Forest retrained on {len(self.window)} samples")

# HalfSpaceTrees — online, no retraining needed
half_space = anomaly.HalfSpaceTrees(
    n_trees=10,
    height=8,
    window_size=100,
    seed=42
)

cusum = CUSUM()
iso_forest = AdaptiveIsolationForest()

# ─── Kafka consumer ───────────────────────────────────
consumer = KafkaConsumer(
    "mcx-crude-prices",
    bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAP"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("CONFLUENT_API_KEY"),
    sasl_plain_password=os.getenv("CONFLUENT_API_SECRET"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

print("Adaptive anomaly detection pipeline started")

# ─── Confirmation delay for drift ─────────────────────
drift_confirmation_count = 0
DRIFT_CONFIRMATION_THRESHOLD = 3  # drift must persist N windows

for msg in consumer:
    tick = msg.value
    price = tick["price"]
    symbol = tick["symbol"]
    ts = tick["timestamp"]
    now = datetime.now(timezone.utc)

    # ── Drift detection ────────────────────────────────
    adwin_drift = adwin.update(price)
    ph_drift = page_hinkley.update(price)

    if adwin_drift or ph_drift:
        drift_confirmation_count += 1
        detector_name = "ADWIN" if adwin_drift else "PageHinkley"
        print(f"Drift signal {drift_confirmation_count}/{DRIFT_CONFIRMATION_THRESHOLD} from {detector_name} at price {price}")
    else:
        drift_confirmation_count = max(0, drift_confirmation_count - 1)

    if drift_confirmation_count >= DRIFT_CONFIRMATION_THRESHOLD:
        print(f"Drift confirmed — recalibrating models")
        cusum.recalibrate()
        iso_forest.retrain()
        drift_confirmation_count = 0

        cur.execute(
            "INSERT INTO drift_events (time, detector, price, message) VALUES (%s, %s, %s, %s)",
            (now, "ADWIN+PH", price, f"Drift confirmed at price {price}")
        )
        conn.commit()

    # ── Anomaly detection ──────────────────────────────
    cusum_anomaly, cusum_score = cusum.update(price)
    iso_anomaly, iso_score = iso_forest.update(price)
    hs_score = half_space.score_one({"price": price})
    half_space.learn_one({"price": price})
    hs_anomaly = hs_score > 0.7

    # Ensemble voting — alert if 2 of 3 agree
    votes = sum([cusum_anomaly, iso_anomaly, hs_anomaly])
    ensemble_anomaly = votes >= 2

    for detector, is_anom, score in [
        ("CUSUM", cusum_anomaly, cusum_score),
        ("IsolationForest", iso_anomaly, iso_score),
        ("HalfSpaceTrees", hs_anomaly, hs_score),
        ("Ensemble", ensemble_anomaly, float(votes)),
    ]:
        cur.execute(
            """INSERT INTO anomaly_alerts
               (time, symbol, price, detector, score, is_anomaly)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (now, symbol, price, detector, float(score), bool(is_anom))
        )

    conn.commit()

    if ensemble_anomaly:
        print(f"ENSEMBLE ALERT: {symbol} @ {price} — {votes}/3 detectors agree")
    elif any([cusum_anomaly, iso_anomaly, hs_anomaly]):
        print(f"Single detector alert: {symbol} @ {price}")
    else:
        print(f"Normal: {symbol} @ {price:.2f}")