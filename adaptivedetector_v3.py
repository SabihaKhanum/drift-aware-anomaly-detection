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

# ─── Hyperparameters ──────────────────────────────────────────────────────────
# CUSUM operates on % returns (e.g. 0.01 = 0.01% move).
#
# IMPORTANT: CUSUM_THRESHOLD and CUSUM_DRIFT_PARAM are now MULTIPLIERS of the
# rolling standard deviation of price_return, not absolute percentage values.
#   - CUSUM_DRIFT_PARAM (k) ~ 0.5  -> slack of 0.5 * rolling_std
#   - CUSUM_THRESHOLD    (h) ~ 5.0 -> alert after cumulative deviation of 5 * rolling_std
# This is the standard CUSUM formulation and keeps the detector scale-invariant,
# since raw price_return is typically ~0.001%-0.05% per tick for BTC — far smaller
# than a fixed absolute slack of 0.5, which previously made CUSUM permanently flat.
DRIFT_CONFIRMATION_THRESHOLD  = int(os.getenv("DRIFT_CONFIRMATION_THRESHOLD", "3"))
CUSUM_THRESHOLD               = float(os.getenv("CUSUM_THRESHOLD", "5.0"))
CUSUM_DRIFT_PARAM             = float(os.getenv("CUSUM_DRIFT_PARAM", "0.5"))
ISO_WINDOW                    = int(os.getenv("ISO_WINDOW", "200"))
ISO_MIN_TRAIN                 = int(os.getenv("ISO_MIN_TRAIN", "50"))
ISO_THRESHOLD                 = float(os.getenv("ISO_THRESHOLD", "-0.6"))   # tightened
ISO_CONTAMINATION             = float(os.getenv("ISO_CONTAMINATION", "0.05"))
HST_THRESHOLD                 = float(os.getenv("HST_THRESHOLD", "0.6"))
VOL_WINDOW                    = int(os.getenv("VOL_WINDOW", "60"))
BASELINE_VOL_WINDOW           = int(os.getenv("BASELINE_VOL_WINDOW", "500"))

# Severity thresholds — calibrate after 48hr run using empirical percentiles
CRISIS_ZSCORE_THRESHOLD       = float(os.getenv("CRISIS_ZSCORE_THRESHOLD", "4.0"))
CRISIS_VOL_RATIO_THRESHOLD    = float(os.getenv("CRISIS_VOL_RATIO_THRESHOLD", "3.0"))
CRISIS_DRIFT_RATE_THRESHOLD   = float(os.getenv("CRISIS_DRIFT_RATE_THRESHOLD", "0.8"))
REGIME_ZSCORE_THRESHOLD       = float(os.getenv("REGIME_ZSCORE_THRESHOLD", "2.5"))
REGIME_VOL_RATIO_THRESHOLD    = float(os.getenv("REGIME_VOL_RATIO_THRESHOLD", "1.5"))
REGIME_ADWIN_WIDTH_THRESHOLD  = int(os.getenv("REGIME_ADWIN_WIDTH_THRESHOLD", "50"))
CRISIS_RECOVERY_STABLE_COUNT  = int(os.getenv("CRISIS_RECOVERY_STABLE_COUNT", "3"))
CRISIS_RECOVERY_VOL_RATIO     = float(os.getenv("CRISIS_RECOVERY_VOL_RATIO", "1.5"))
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
    Tracks rolling statistics over short and long windows.
    Operates on price_return (% change) not raw price,
    so vol_ratio and z_score are scale-invariant.
    """
    def __init__(self, short_window=60, long_window=500):
        self.short = deque(maxlen=short_window)
        self.long  = deque(maxlen=long_window)
        self.drift_signals = deque(maxlen=20)

    def update(self, value, drift_signal: bool):
        # value should be price_return, not raw price
        self.short.append(value)
        self.long.append(value)
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

    def z_score(self, value):
        std = self.rolling_vol
        if std < 1e-9:
            return 0.0
        return (value - self.rolling_mean) / std

    @property
    def drift_rate(self):
        if not self.drift_signals:
            return 0.0
        return float(np.mean(self.drift_signals))


# ─── Severity classifier ──────────────────────────────────────────────────────
def classify_severity(stats: RollingStats, value: float, adwin_width: int) -> str:
    """
    Classify confirmed drift into severity tier using return-based features.
    value: the current price_return (not raw price)
    """
    vr = stats.vol_ratio
    zs = abs(stats.z_score(value))
    dr = stats.drift_rate

    if zs > CRISIS_ZSCORE_THRESHOLD or vr > CRISIS_VOL_RATIO_THRESHOLD or dr > CRISIS_DRIFT_RATE_THRESHOLD:
        return "CRISIS"
    if vr > REGIME_VOL_RATIO_THRESHOLD or zs > REGIME_ZSCORE_THRESHOLD or adwin_width < REGIME_ADWIN_WIDTH_THRESHOLD:
        return "REGIME_SHIFT"
    return "INTRADAY_NOISE"


# ─── CUSUM ────────────────────────────────────────────────────────────────────
class CUSUM:
    """
    Two-sided CUSUM on % price returns, scaled by an EXTERNAL, stable
    baseline standard deviation of returns (not its own recent history).

    FIX HISTORY:
    1) Originally `drift_param`/`threshold` were fixed absolute percentage
       values (0.5, 5.0), but tick-to-tick BTC returns are ~0.001%-0.05% —
       10-500x smaller — so CUSUM could never accumulate past zero.
    2) Scaling by CUSUM's own short internal history fixed (1), but
       introduced a new problem: during a real, sustained move, that same
       short window becomes contaminated by the move itself, so `std`
       inflates in lockstep with the signal CUSUM is supposed to detect.
       The threshold becomes a moving goalpost that runs away from genuine
       drift — a real ~1.5% rally in BTC still got graded against a
       just-as-elevated local std and CUSUM stayed flat.

    THE FIX: CUSUM no longer computes its own std. It's given an external,
    longer-horizon baseline std each tick (via `set_baseline_std`) — in this
    pipeline, RollingStats.baseline_vol, the 500-tick window. A 500-tick
    baseline is diluted by far more "normal" history than a 20-30 tick
    window, so it doesn't run away the instant a real move starts, and a
    genuine sustained shift can actually cross k/h multiples of it.
        slack     = k_multiplier * baseline_std
        threshold = h_multiplier * baseline_std
    """
    def __init__(self, k_multiplier=0.5, h_multiplier=5.0, min_std=1e-6, history_len=100):
        self.k_multiplier    = k_multiplier
        self.baseline_h      = h_multiplier
        self.h_multiplier    = h_multiplier   # current (possibly widened) h multiplier
        self.min_std         = min_std
        self.mean            = None
        self.cusum_pos       = 0.0
        self.cusum_neg       = 0.0
        self.history         = deque(maxlen=history_len)
        self.frozen          = False
        self.baseline_std    = min_std   # set externally each tick before update()

    def set_baseline_std(self, baseline_std: float):
        """Called once per tick with a stable, longer-horizon volatility
        estimate (e.g. RollingStats.baseline_vol) so CUSUM's own threshold
        doesn't get contaminated by the very move it's trying to detect."""
        self.baseline_std = max(float(baseline_std), self.min_std)

    def update(self, value):
        if self.frozen:
            return True, 99.0

        self.history.append(value)

        if self.mean is None:
            # Warm up the reference mean over the first few ticks, then
            # freeze it — it must NOT be recalculated every tick, or it
            # chases the very drift it's supposed to detect (see docstring).
            if len(self.history) < 10:
                self.mean = float(np.mean(self.history))
                return False, 0.0
            self.mean = float(np.mean(self.history))

        std = self.baseline_std
        k   = self.k_multiplier * std
        h   = self.h_multiplier * std

        deviation      = value - self.mean
        self.cusum_pos = max(0.0, self.cusum_pos + deviation - k)
        self.cusum_neg = max(0.0, self.cusum_neg - deviation - k)

        is_anomaly = self.cusum_pos > h or self.cusum_neg > h
        # NOTE: self.mean is intentionally NOT updated here. It only moves
        # during adapt_regime()/adapt_intraday()/adapt_crisis_recover(),
        # i.e. after a drift has actually been confirmed. Recalculating it
        # every tick (the previous bug) made CUSUM compare each value to the
        # rolling average of its own recent window — which drifts along with
        # any real trend, permanently masking sustained drift as "no change."

        # Normalize score by h so it's on a comparable ~0-1+ scale to the
        # other detectors instead of raw cusum units.
        raw_score = max(self.cusum_pos, self.cusum_neg)
        score     = raw_score / max(h, 1e-9)
        return is_anomaly, score

    def adapt_intraday(self):
        self.cusum_pos    = 0.0
        self.cusum_neg    = 0.0
        self.h_multiplier = self.baseline_h * INTRADAY_CUSUM_WIDEN_FACTOR
        log.info("CUSUM soft adapt — h_multiplier widened to %.2f", self.h_multiplier)

    def adapt_regime(self):
        if len(self.history) > 10:
            self.mean         = float(np.mean(self.history))
            self.cusum_pos    = 0.0
            self.cusum_neg    = 0.0
            self.h_multiplier = self.baseline_h
            log.info("CUSUM regime recalibration — mean=%.6f", self.mean)

    def adapt_crisis_freeze(self):
        self.frozen = True
        log.warning("CUSUM FROZEN — crisis mode active")

    def adapt_crisis_recover(self):
        self.frozen = False
        self.adapt_regime()
        log.info("CUSUM unfrozen and recalibrated")


# ─── EMA smoother (for CUSUM input only) ──────────────────────────────────────
class EMA:
    """
    Exponential moving average. Used ONLY to smooth price_return before it
    reaches CUSUM.

    WHY: CUSUM accumulates deviations that share the same sign in a row. If
    consecutive ticks alternate (+d, -d, +d, -d, ...) — e.g. bid/ask flicker,
    duplicate/stale prints, or any rapid back-and-forth in the raw feed —
    cusum_pos/cusum_neg get pulled back toward 0 almost every other tick and
    can never accumulate past threshold, no matter how k/h are scaled. This
    isn't a tuning problem, it's that the raw series has no sustained
    one-directional run for CUSUM to detect.

    Smoothing price_return with a short EMA before CUSUM averages out
    tick-to-tick alternation while still tracking genuine sustained moves
    (a real trend keeps pushing the EMA in one direction; alternation
    averages toward ~0). ADWIN/PageHinkley/HST are left on the raw series
    since they don't rely on directional accumulation and are already
    reacting correctly to the raw ticks.
    """
    def __init__(self, span=5):
        self.alpha = 2.0 / (span + 1.0)
        self.value = None

    def update(self, x):
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1 - self.alpha) * self.value
        return self.value


CUSUM_EMA_SPAN = int(os.getenv("CUSUM_EMA_SPAN", "5"))


# ─── Isolation Forest ─────────────────────────────────────────────────────────
class AdaptiveIsolationForest:
    """
    Batch anomaly detector on raw price.
    ISO handles absolute price values well — no normalisation needed.
    Retrained on confirmed non-crisis drift.
    """
    def __init__(self, window_size=200, min_train=50,
                 threshold=-0.6, contamination=0.05):
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
        log.info("IsolationForest: no action for intraday noise")

    def adapt_regime(self):
        if len(self.window) >= self.min_train:
            self._train()

    def adapt_crisis_freeze(self):
        self.frozen = True
        log.warning("IsolationForest FROZEN — crisis mode active")

    def adapt_crisis_recover(self):
        self.frozen = False
        self.adapt_regime()
        log.info("IsolationForest unfrozen and retrained")


# ─── Initialise detectors ─────────────────────────────────────────────────────
adwin        = drift.ADWIN()
page_hinkley = drift.PageHinkley()

# HalfSpaceTrees on price_return — scale-invariant, warms up faster
half_space = anomaly.HalfSpaceTrees(
    n_trees=10, height=8, window_size=100, seed=42
)

cusum      = CUSUM(k_multiplier=CUSUM_DRIFT_PARAM, h_multiplier=CUSUM_THRESHOLD)
cusum_ema  = EMA(span=CUSUM_EMA_SPAN)
iso_forest = AdaptiveIsolationForest(
    window_size=ISO_WINDOW, min_train=ISO_MIN_TRAIN,
    threshold=ISO_THRESHOLD, contamination=ISO_CONTAMINATION
)
stats      = RollingStats(short_window=VOL_WINDOW, long_window=BASELINE_VOL_WINDOW)

# ─── Crisis state ─────────────────────────────────────────────────────────────
crisis_state = {"active": False, "onset_time": None, "stable_count": 0}


# ─── Adaptation dispatcher ────────────────────────────────────────────────────
def dispatch_adaptation(severity, cusum, iso_forest, crisis_state, now):
    if severity == "INTRADAY_NOISE":
        cusum.adapt_intraday()
        iso_forest.adapt_intraday()
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


def check_crisis_recovery(stats, cusum, iso_forest, crisis_state):
    if not crisis_state["active"]:
        return False
    if stats.vol_ratio < CRISIS_RECOVERY_VOL_RATIO:
        crisis_state["stable_count"] += 1
    else:
        crisis_state["stable_count"] = 0
    if crisis_state["stable_count"] >= CRISIS_RECOVERY_STABLE_COUNT:
        log.info("Crisis resolved — retraining models")
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
prev_price               = None   # for price_return calculation

# ─── Main loop ────────────────────────────────────────────────────────────────
for msg in consumer:
    tick   = msg.value
    price  = float(tick["price"])
    symbol = tick.get("symbol", "BTCUSDT")
    now    = datetime.now(timezone.utc)
    total_ticks += 1

    inserts = []

    # ── Compute % return — used by CUSUM, HST, drift detectors, RollingStats ──
    if prev_price is not None and prev_price > 0:
        price_return = (price - prev_price) / prev_price * 100.0
    else:
        price_return = 0.0
    prev_price = price

    # ── Crisis recovery check ─────────────────────────────────────────────────
    if crisis_state["active"]:
        stats.update(price_return, drift_signal=False)
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
        stats.update(price_return, drift_signal=False)

    # ── Drift detection on price_return ───────────────────────────────────────
    if ADAPTIVE_MODE and not crisis_state["active"]:
        adwin_drift = adwin.update(price_return)        # ← price_return not price
        ph_drift    = page_hinkley.update(price_return) # ← price_return not price
        any_drift_signal = adwin_drift or ph_drift

        if any_drift_signal:
            stats.update(price_return, drift_signal=True)
            if drift_confirmation_count == 0:
                drift_first_signal_time = now
            drift_confirmation_count += 1
            triggering = "ADWIN" if adwin_drift else "PageHinkley"
            log.info("Drift signal %d/%d from %s — return=%.6f%%",
                     drift_confirmation_count, DRIFT_CONFIRMATION_THRESHOLD,
                     triggering, price_return)
        else:
            drift_confirmation_count = max(0, drift_confirmation_count - 1)
            if drift_confirmation_count == 0:
                drift_first_signal_time = None

        if drift_confirmation_count >= DRIFT_CONFIRMATION_THRESHOLD:
            adaptation_latency_s = (
                (now - drift_first_signal_time).total_seconds()
                if drift_first_signal_time else None
            )
            adwin_width = getattr(adwin, "width", -1)
            severity    = classify_severity(stats, price_return, adwin_width)

            log.info("Drift CONFIRMED — severity=%s latency=%.3fs vol_ratio=%.2f z=%.2f",
                     severity, adaptation_latency_s or 0.0,
                     stats.vol_ratio, stats.z_score(price_return))

            strategy = dispatch_adaptation(severity, cusum, iso_forest, crisis_state, now)

            inserts.append((
                "INSERT INTO drift_events "
                "(time, detector, price, message, confirmation_count, "
                " first_signal_time, adaptation_latency_s, adaptive_mode, "
                " severity, adaptation_strategy, vol_ratio, z_score, "
                " drift_rate, adwin_width, crisis_mode) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (now, "ADWIN+PH", price,
                 f"Drift confirmed — {severity} — {strategy}",
                 drift_confirmation_count,
                 drift_first_signal_time,
                 adaptation_latency_s,
                 ADAPTIVE_MODE,
                 severity,
                 strategy,
                 stats.vol_ratio,
                 stats.z_score(price_return),
                 stats.drift_rate,
                 adwin_width,
                 crisis_state["active"])
            ))

            drift_confirmation_count = 0
            drift_first_signal_time  = None

    # ── Anomaly detection ─────────────────────────────────────────────────────
    # CUSUM    — EMA-smoothed price_return (span=CUSUM_EMA_SPAN), scaled by
    #            RollingStats' long-window (500-tick) baseline_vol. Smoothing
    #            is necessary because raw tick-to-tick alternation (flicker)
    #            cancels out same-tick before CUSUM's directional accumulation
    #            can build up — see EMA class docstring above.
    # ISO      — raw price, batch model handles absolute values fine
    # HST      — raw price_return, online model, scale-invariant, unaffected
    #            by alternation since it doesn't rely on directional runs

    smoothed_return = cusum_ema.update(price_return)
    cusum.set_baseline_std(stats.baseline_vol)
    cusum_anomaly, cusum_score = cusum.update(smoothed_return)

    iso_anomaly, iso_score = iso_forest.update(price)

    hs_score   = half_space.score_one({"value": price_return})  # ← "value" key, price_return
    half_space.learn_one({"value": price_return})               # ← consistent key
    hs_anomaly = hs_score > HST_THRESHOLD or crisis_state["active"]

    votes            = sum([cusum_anomaly, iso_anomaly, hs_anomaly])
    ensemble_anomaly = votes >= 2

    if ensemble_anomaly:
        total_ensemble_anomalies += 1

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

    # ── Single commit per tick ────────────────────────────────────────────────
    for sql, params in inserts:
        cur.execute(sql, params)
    conn.commit()

    # ── Debug log every 50 ticks ──────────────────────────────────────────────
    if total_ticks % 50 == 0:
        log.info(
            "tick=%d price=%.2f return=%.6f%% cusum=%.4f iso_score=%.4f "
            "hst=%.4f votes=%d anomaly_rate=%.1f%% drift_events=%d",
            total_ticks, price, price_return,
            cusum_score, iso_score, hs_score, votes,
            100 * total_ensemble_anomalies / total_ticks,
            drift_confirmation_count
        )

    if ensemble_anomaly:
        log.warning("ENSEMBLE ALERT: %s @ %.2f  return=%.4f%%  votes=%d/3  crisis=%s",
                    symbol, price, price_return, votes, crisis_state["active"])