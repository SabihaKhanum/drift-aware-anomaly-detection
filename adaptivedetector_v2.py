"""
Drift-Aware Anomaly Detection Pipeline
=======================================
Live stream: Binance BTC/USDT via Kafka → TimescaleDB
Runtime:     Python streaming pipeline (kafka-python, River, scikit-learn)

Drift severity tiers:
  INTRADAY_NOISE  — transient microstructure noise, soft adaptation
  REGIME_SHIFT    — sustained distributional change, standard recalibration
  CRISIS          — structural break / flash crash, model freeze + delayed retrain

Authors: Sabiha Khanum Z
"""

import os
import json
import logging
import numpy as np
from datetime import datetime, timezone
from collections import deque
from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg2
from river import drift, anomaly
from sklearn.ensemble import IsolationForest

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── Mode flags ───────────────────────────────────────────────────────────────
ADAPTIVE_MODE = os.getenv("ADAPTIVE_MODE", "true").lower() == "true"
log.info("Mode: %s", "ADAPTIVE" if ADAPTIVE_MODE else "STATIC BASELINE")

# ─── Hyperparameters (all overridable via .env) ───────────────────────────────
DRIFT_CONFIRMATION_THRESHOLD  = int(os.getenv("DRIFT_CONFIRMATION_THRESHOLD", "3"))
CUSUM_THRESHOLD               = float(os.getenv("CUSUM_THRESHOLD", "500"))
CUSUM_DRIFT_PARAM             = float(os.getenv("CUSUM_DRIFT_PARAM", "50"))
ISO_WINDOW                    = int(os.getenv("ISO_WINDOW", "200"))
ISO_MIN_TRAIN                 = int(os.getenv("ISO_MIN_TRAIN", "50"))
ISO_THRESHOLD                 = float(os.getenv("ISO_THRESHOLD", "-0.5"))
ISO_CONTAMINATION             = float(os.getenv("ISO_CONTAMINATION", "0.05"))
HST_THRESHOLD                 = float(os.getenv("HST_THRESHOLD", "0.7"))
VOL_WINDOW                    = int(os.getenv("VOL_WINDOW", "60"))       # rolling vol window
BASELINE_VOL_WINDOW           = int(os.getenv("BASELINE_VOL_WINDOW", "500"))  # longer baseline

# Severity classification thresholds
# Set empirically: run 48hrs, inspect vol_ratio / z_score distribution,
# then replace these with percentile-justified values.
CRISIS_ZSCORE_THRESHOLD       = float(os.getenv("CRISIS_ZSCORE_THRESHOLD", "4.0"))
CRISIS_VOL_RATIO_THRESHOLD    = float(os.getenv("CRISIS_VOL_RATIO_THRESHOLD", "3.0"))
CRISIS_DRIFT_RATE_THRESHOLD   = float(os.getenv("CRISIS_DRIFT_RATE_THRESHOLD", "0.8"))
REGIME_ZSCORE_THRESHOLD       = float(os.getenv("REGIME_ZSCORE_THRESHOLD", "2.5"))
REGIME_VOL_RATIO_THRESHOLD    = float(os.getenv("REGIME_VOL_RATIO_THRESHOLD", "1.5"))
REGIME_ADWIN_WIDTH_THRESHOLD  = int(os.getenv("REGIME_ADWIN_WIDTH_THRESHOLD", "50"))

# Post-crisis recovery: require N consecutive stable windows before retraining
CRISIS_RECOVERY_STABLE_COUNT  = int(os.getenv("CRISIS_RECOVERY_STABLE_COUNT", "3"))
CRISIS_RECOVERY_VOL_RATIO     = float(os.getenv("CRISIS_RECOVERY_VOL_RATIO", "1.5"))

# Intraday: CUSUM threshold multiplier during soft adaptation
INTRADAY_CUSUM_WIDEN_FACTOR   = float(os.getenv("INTRADAY_CUSUM_WIDEN_FACTOR", "1.2"))


# ─── Database ─────────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"),
    port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"),
    password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB"),
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS drift_events (
    time                  TIMESTAMPTZ NOT NULL,
    detector              TEXT,
    price                 DOUBLE PRECISION,
    message               TEXT,
    confirmation_count    INT,
    first_signal_time     TIMESTAMPTZ,
    adaptation_latency_s  DOUBLE PRECISION,
    adaptive_mode         BOOLEAN,
    -- severity fields
    severity              TEXT,
    adaptation_strategy   TEXT,
    vol_ratio             DOUBLE PRECISION,
    z_score               DOUBLE PRECISION,
    drift_rate            DOUBLE PRECISION,
    adwin_width           INT,
    crisis_mode           BOOLEAN DEFAULT FALSE
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    time               TIMESTAMPTZ NOT NULL,
    symbol             TEXT,
    price              DOUBLE PRECISION,
    detector           TEXT,
    score              DOUBLE PRECISION,
    is_anomaly         BOOLEAN,
    adaptive_mode      BOOLEAN,
    crisis_mode        BOOLEAN DEFAULT FALSE,
    severity_context   TEXT
);
""")

# for table in ["drift_events", "anomaly_alerts"]:
#     cur.execute(f"SELECT create_hypertable('{table}', 'time', if_not_exists => TRUE);")

for table in ["drift_events", "anomaly_alerts"]:
    cur.execute(f"""
        SELECT create_hypertable('{table}', 'time', 
               if_not_exists => TRUE,
               migrate_data => TRUE);
    """)

conn.commit()
log.info("Database ready")


# ─── Rolling statistics ───────────────────────────────────────────────────────
class RollingStats:
    """
    Tracks rolling mean, std, and volatility over a sliding window.
    Used for severity classification features.
    """
    def __init__(self, short_window=60, long_window=500):
        self.short = deque(maxlen=short_window)
        self.long  = deque(maxlen=long_window)
        # Drift signal history for drift_rate computation
        self.drift_signals = deque(maxlen=20)

    def update(self, price, drift_signal: bool):
        self.short.append(price)
        self.long.append(price)
        self.drift_signals.append(1 if drift_signal else 0)

    @property
    def rolling_vol(self):
        return float(np.std(self.short)) if len(self.short) > 1 else 0.0

    @property
    def baseline_vol(self):
        return float(np.std(self.long)) if len(self.long) > 1 else 1e-9

    @property
    def rolling_mean(self):
        return float(np.mean(self.short)) if self.short else 0.0

    @property
    def vol_ratio(self):
        return self.rolling_vol / max(self.baseline_vol, 1e-9)

    def z_score(self, price):
        std = self.rolling_vol
        if std < 1e-9:
            return 0.0
        return (price - self.rolling_mean) / std

    @property
    def drift_rate(self):
        if not self.drift_signals:
            return 0.0
        return float(np.mean(self.drift_signals))

    def price_velocity(self, price, k=5):
        if len(self.short) < k:
            return 0.0
        return abs(price - list(self.short)[-k]) / k


# ─── Severity classifier ──────────────────────────────────────────────────────
def classify_severity(stats: RollingStats, price: float, adwin_width: int) -> str:
    """
    Classify confirmed drift into one of three tiers based on market microstructure features.

    Returns: 'INTRADAY_NOISE' | 'REGIME_SHIFT' | 'CRISIS'

    Thresholds are hyperparameters — calibrate against your empirical score distributions.
    See Section 3.7 of the paper for threshold justification methodology.
    """
    vr  = stats.vol_ratio
    zs  = abs(stats.z_score(price))
    dr  = stats.drift_rate

    # Crisis: any one of — extreme z-score, volatility explosion, rapid repeated drift
    if zs > CRISIS_ZSCORE_THRESHOLD or vr > CRISIS_VOL_RATIO_THRESHOLD or dr > CRISIS_DRIFT_RATE_THRESHOLD:
        return "CRISIS"

    # Regime shift: sustained elevated volatility or medium deviation or sharp ADWIN window change
    if vr > REGIME_VOL_RATIO_THRESHOLD or zs > REGIME_ZSCORE_THRESHOLD or adwin_width < REGIME_ADWIN_WIDTH_THRESHOLD:
        return "REGIME_SHIFT"

    # Default: transient microstructure noise
    return "INTRADAY_NOISE"


# ─── CUSUM ────────────────────────────────────────────────────────────────────
class CUSUM:
    """
    Two-sided CUSUM control chart.
    threshold:   alert when accumulator exceeds this (default 5.0)
    drift_param: allowance k — set to ~0.5 * expected shift size
    """
    def __init__(self, threshold=5.0, drift_param=0.5):
        self.baseline_threshold = threshold
        self.threshold   = threshold
        self.drift_param = drift_param
        self.mean        = None
        self.cusum_pos   = 0.0
        self.cusum_neg   = 0.0
        self.history     = deque(maxlen=100)
        self.frozen      = False    # set True during crisis

    def update(self, value):
        if self.frozen:
            # During crisis: report all as anomalous, don't update state
            return True, 99.0

        self.history.append(value)
        if self.mean is None:
            self.mean = value
            return False, 0.0

        deviation      = value - self.mean
        self.cusum_pos = max(0.0, self.cusum_pos + deviation   - self.drift_param)
        self.cusum_neg = max(0.0, self.cusum_neg - deviation   - self.drift_param)

        is_anomaly = self.cusum_pos > self.threshold or self.cusum_neg > self.threshold
        self.mean  = float(np.mean(self.history))
        score      = max(self.cusum_pos, self.cusum_neg)
        return is_anomaly, score

    # ── Adaptation strategies ─────────────────────────────────────────────────
    def adapt_intraday(self):
        """Soft adaptation: widen threshold, reset accumulators, keep mean."""
        self.cusum_pos = 0.0
        self.cusum_neg = 0.0
        self.threshold = self.baseline_threshold * INTRADAY_CUSUM_WIDEN_FACTOR
        log.info("CUSUM soft adapt (intraday) — threshold widened to %.2f", self.threshold)

    def adapt_regime(self):
        """Standard recalibration: recompute mean from history, reset accumulators."""
        if len(self.history) > 10:
            self.mean      = float(np.mean(self.history))
            self.cusum_pos = 0.0
            self.cusum_neg = 0.0
            self.threshold = self.baseline_threshold   # restore baseline threshold
            log.info("CUSUM regime recalibration — mean=%.4f", self.mean)

    def adapt_crisis_freeze(self):
        """Crisis: freeze model. All outputs → anomalous until unfrozen."""
        self.frozen = True
        log.warning("CUSUM FROZEN — crisis mode active")

    def adapt_crisis_recover(self):
        """Post-crisis: unfreeze and recalibrate on stabilised data."""
        self.frozen = False
        self.adapt_regime()
        log.info("CUSUM unfrozen and recalibrated after crisis resolution")


# ─── Isolation Forest ─────────────────────────────────────────────────────────
class AdaptiveIsolationForest:
    """
    Batch anomaly detector with severity-typed adaptation.
    Maintains a rolling window; retrains on confirmed non-crisis drift.
    During crisis: model is frozen, all outputs → anomalous.
    """
    def __init__(self, window_size=200, min_train=50,
                 threshold=-0.5, contamination=0.05):
        self.window        = deque(maxlen=window_size)
        self.min_train     = min_train
        self.threshold     = threshold
        self.contamination = contamination
        self.model         = None
        self.trained       = False
        self.frozen        = False
        self.train_count   = 0

    def update(self, value):
        self.window.append([value])

        if self.frozen:
            return True, 1.0

        if len(self.window) >= self.min_train and not self.trained:
            self._train()
        if not self.trained:
            return False, 0.0

        score = float(self.model.score_samples([[value]])[0])
        return score < self.threshold, abs(score)

    def _train(self):
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100
        )
        self.model.fit(list(self.window))
        self.trained     = True
        self.train_count += 1
        log.info("IsolationForest trained (run #%d) on %d samples",
                 self.train_count, len(self.window))

    def adapt_intraday(self):
        """No retrain — window absorbs noise naturally."""
        log.info("IsolationForest: no action for intraday noise")

    def adapt_regime(self):
        """Full retrain on most recent window."""
        if len(self.window) >= self.min_train:
            self._train()

    def adapt_crisis_freeze(self):
        self.frozen = True
        log.warning("IsolationForest FROZEN — crisis mode active")

    def adapt_crisis_recover(self):
        """Unfreeze and retrain on post-crisis stabilised data."""
        self.frozen = False
        self.adapt_regime()
        log.info("IsolationForest unfrozen and retrained after crisis resolution")


# ─── Initialise detectors ─────────────────────────────────────────────────────
adwin       = drift.ADWIN()
page_hinkley= drift.PageHinkley()

half_space  = anomaly.HalfSpaceTrees(
    n_trees=10, height=8, window_size=100, seed=42
)

cusum       = CUSUM(threshold=CUSUM_THRESHOLD, drift_param=CUSUM_DRIFT_PARAM)
iso_forest  = AdaptiveIsolationForest(
    window_size=ISO_WINDOW, min_train=ISO_MIN_TRAIN,
    threshold=ISO_THRESHOLD, contamination=ISO_CONTAMINATION
)

stats       = RollingStats(short_window=VOL_WINDOW, long_window=BASELINE_VOL_WINDOW)

# ─── Crisis state ─────────────────────────────────────────────────────────────
crisis_state = {
    "active":       False,
    "onset_time":   None,
    "stable_count": 0,
}


# ─── Adaptation dispatcher ────────────────────────────────────────────────────
def dispatch_adaptation(severity: str, cusum: CUSUM,
                        iso_forest: AdaptiveIsolationForest,
                        crisis_state: dict, now: datetime) -> str:
    """
    Apply the correct adaptation strategy for the classified severity tier.
    Returns the strategy name for logging.
    """
    if severity == "INTRADAY_NOISE":
        cusum.adapt_intraday()
        iso_forest.adapt_intraday()
        # HalfSpaceTrees adapts online — no action needed
        return "SOFT_ADAPT"

    elif severity == "REGIME_SHIFT":
        cusum.adapt_regime()
        iso_forest.adapt_regime()
        return "STANDARD_RECALIBRATION"

    elif severity == "CRISIS":
        cusum.adapt_crisis_freeze()
        iso_forest.adapt_crisis_freeze()
        crisis_state["active"]       = True
        crisis_state["onset_time"]   = now
        crisis_state["stable_count"] = 0
        return "CRISIS_FREEZE"

    return "UNKNOWN"


def check_crisis_recovery(stats: RollingStats, cusum: CUSUM,
                           iso_forest: AdaptiveIsolationForest,
                           crisis_state: dict) -> bool:
    """
    Poll for crisis resolution. Returns True if recovery triggered.
    Requires CRISIS_RECOVERY_STABLE_COUNT consecutive windows with
    vol_ratio < CRISIS_RECOVERY_VOL_RATIO before retraining.
    """
    if not crisis_state["active"]:
        return False

    if stats.vol_ratio < CRISIS_RECOVERY_VOL_RATIO:
        crisis_state["stable_count"] += 1
    else:
        crisis_state["stable_count"] = 0

    if crisis_state["stable_count"] >= CRISIS_RECOVERY_STABLE_COUNT:
        log.info("Crisis resolved after %d stable windows — retraining models",
                 CRISIS_RECOVERY_STABLE_COUNT)
        cusum.adapt_crisis_recover()
        iso_forest.adapt_crisis_recover()
        crisis_state["active"]       = False
        crisis_state["onset_time"]   = None
        crisis_state["stable_count"] = 0
        return True

    return False


# ─── Kafka consumer ───────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    "binance-btcusdt",
    bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAP"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    group_id=f"detector-{'adaptive' if ADAPTIVE_MODE else 'static'}",
    sasl_plain_username=os.getenv("CONFLUENT_API_KEY"),
    sasl_plain_password=os.getenv("CONFLUENT_API_SECRET"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

log.info("Pipeline consuming from binance-btcusdt")

# ─── Pipeline state ───────────────────────────────────────────────────────────
drift_confirmation_count = 0
drift_first_signal_time  = None
total_ticks              = 0
total_ensemble_anomalies = 0
prev_price               = None

# ─── Main loop ────────────────────────────────────────────────────────────────
for msg in consumer:
    tick   = msg.value
    price  = float(tick["price"])
    symbol = tick.get("symbol", "BTCUSDT")
    now    = datetime.now(timezone.utc)
    total_ticks += 1

    # Any drift signal this tick (needed by RollingStats before drift block)
    any_drift_signal = False
    inserts = []

    # ── Crisis recovery check (runs every tick when crisis is active) ──────────
    if crisis_state["active"]:
        stats.update(price, drift_signal=False)
        recovered = check_crisis_recovery(stats, cusum, iso_forest, crisis_state)
        if recovered:
            inserts.append((
                "INSERT INTO drift_events "
                "(time, detector, price, message, adaptive_mode, severity, "
                " adaptation_strategy, vol_ratio, crisis_mode) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (now, "RECOVERY", price, "Crisis resolved — models retrained",
                 ADAPTIVE_MODE, "CRISIS_RESOLVED", "POST_CRISIS_RETRAIN",
                 stats.vol_ratio, False)
            ))
    else:
        stats.update(price, drift_signal=False)  # will update again below if drift

    # ── Drift detection ────────────────────────────────────────────────────────
    if ADAPTIVE_MODE and not crisis_state["active"]:
        adwin_drift = adwin.update(price)
        ph_drift    = page_hinkley.update(price)
        any_drift_signal = adwin_drift or ph_drift

        # Update stats with drift signal flag for drift_rate feature
        if any_drift_signal:
            stats.update(price, drift_signal=True)

        if any_drift_signal:
            if drift_confirmation_count == 0:
                drift_first_signal_time = now
            drift_confirmation_count += 1
            triggering = "ADWIN" if adwin_drift else "PageHinkley"
            log.info("Drift signal %d/%d from %s @ %.4f",
                     drift_confirmation_count, DRIFT_CONFIRMATION_THRESHOLD,
                     triggering, price)
        else:
            drift_confirmation_count = max(0, drift_confirmation_count - 1)
            if drift_confirmation_count == 0:
                drift_first_signal_time = None

        if drift_confirmation_count >= DRIFT_CONFIRMATION_THRESHOLD:
            # ── Measure adaptation latency ─────────────────────────────────
            adaptation_latency_s = (
                (now - drift_first_signal_time).total_seconds()
                if drift_first_signal_time else None
            )

            # ── Classify severity ──────────────────────────────────────────
            adwin_width = getattr(adwin, "width", -1)
            severity    = classify_severity(stats, price, adwin_width)

            log.info("Drift CONFIRMED — severity=%s latency=%.3fs vol_ratio=%.2f z=%.2f",
                     severity, adaptation_latency_s or 0.0,
                     stats.vol_ratio, stats.z_score(price))

            # ── Dispatch adaptation ────────────────────────────────────────
            strategy = dispatch_adaptation(
                severity, cusum, iso_forest, crisis_state, now
            )

            inserts.append((
                "INSERT INTO drift_events "
                "(time, detector, price, message, confirmation_count, "
                " first_signal_time, adaptation_latency_s, adaptive_mode, "
                " severity, adaptation_strategy, vol_ratio, z_score, "
                " drift_rate, adwin_width, crisis_mode) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (now, "ADWIN+PH", price,
                 f"Drift confirmed — {severity} — strategy: {strategy}",
                 drift_confirmation_count,
                 drift_first_signal_time,
                 adaptation_latency_s,
                 ADAPTIVE_MODE,
                 severity,
                 strategy,
                 stats.vol_ratio,
                 stats.z_score(price),
                 stats.drift_rate,
                 adwin_width,
                 crisis_state["active"])
            ))

            drift_confirmation_count = 0
            drift_first_signal_time  = None

    # ── Anomaly detection (all ticks, all modes) ───────────────────────────────
    # cusum_anomaly,  cusum_score  = cusum.update(price)
    # iso_anomaly,    iso_score    = iso_forest.update(price)
    # hs_score = half_space.score_one({"price": price})
    # half_space.learn_one({"price": price})
    # hs_anomaly = hs_score > HST_THRESHOLD or crisis_state["active"]
# ── Normalise price to returns for CUSUM ──────────────
    if prev_price is not None and prev_price > 0:
        price_return = (price - prev_price) / prev_price * 100
    else:
        price_return = 0.0
    prev_price = price

    # ── Anomaly detection ──────────────────────────────────
    cusum_anomaly,  cusum_score  = cusum.update(price_return)  # returns, not raw price
    iso_anomaly,    iso_score    = iso_forest.update(price)     # ISO handles raw price fine
    hs_score = half_space.score_one({"price": price})
    half_space.learn_one({"price": price})
    hs_anomaly = hs_score > HST_THRESHOLD or crisis_state["active"]

    votes            = sum([cusum_anomaly, iso_anomaly, hs_anomaly])
    ensemble_anomaly = votes >= 2

    if ensemble_anomaly:
        total_ensemble_anomalies += 1

    # Severity context for anomaly log (last known severity, or NORMAL)
    severity_ctx = "CRISIS" if crisis_state["active"] else "NORMAL"

    for detector, is_anom, score in [
        ("CUSUM",           cusum_anomaly,    cusum_score),
        ("IsolationForest", iso_anomaly,      iso_score),
        ("HalfSpaceTrees",  hs_anomaly,       hs_score),
        ("Ensemble",        ensemble_anomaly, float(votes)),
    ]:
        inserts.append((
            "INSERT INTO anomaly_alerts "
            "(time, symbol, price, detector, score, is_anomaly, "
            " adaptive_mode, crisis_mode, severity_context) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (now, symbol, price, detector, float(score),
             bool(is_anom), ADAPTIVE_MODE,
             crisis_state["active"], severity_ctx)
        ))

    # ── Single commit per tick ─────────────────────────────────────────────────
    for sql, params in inserts:
        cur.execute(sql, params)
    conn.commit()

    # ── Periodic logging ───────────────────────────────────────────────────────
    if ensemble_anomaly:
        log.warning("ENSEMBLE ALERT: %s @ %.4f  votes=%d/3  crisis=%s",
                    symbol, price, votes, crisis_state["active"])
    elif total_ticks % 100 == 0:
        log.info("tick=%d price=%.4f vol_ratio=%.2f anomaly_rate=%.2f%% crisis=%s",
                 total_ticks, price, stats.vol_ratio,
                 100 * total_ensemble_anomalies / total_ticks,
                 crisis_state["active"])