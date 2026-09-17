# import os
# import sys
# import logging
# import numpy as np
# import pandas as pd
# from datetime import timezone
# from collections import deque
# from dotenv import load_dotenv
# import psycopg2
# from river import drift, anomaly
# from sklearn.ensemble import IsolationForest

# load_dotenv()

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%dT%H:%M:%S"
# )
# log = logging.getLogger(__name__)

# # ═══════════════════════════════════════════════════════════════════════════
# # BACKTEST HARNESS
# #
# # Replays historical 1-minute bars through the SAME (fixed) detector logic
# # as the live Kafka pipeline, and writes to the SAME drift_events /
# # anomaly_alerts TimescaleDB tables. Nothing about CUSUM/ADWIN/PageHinkley/
# # IsolationForest/HalfSpaceTrees logic differs from the live pipeline —
# # only the data source (CSV instead of Kafka) and the timestamp (the bar's
# # own historical timestamp instead of datetime.now()).
# #
# # Because it writes into the same tables, backtest rows and live rows are
# # naturally separated by `time` — e.g. filter WHERE time < '2023-01-01'
# # for this 2022 backtest vs. live 2026 data. No schema change needed.
# #
# # CSV INPUT: set BACKTEST_CSV_PATH to a 1-minute OHLCV CSV (e.g. from
# # data.binance.vision klines or CryptoDataDownload). The script looks for
# # a timestamp column among {time, timestamp, open_time, date} and a price
# # column among {close, price, Close}. Adjust COLUMN NAMES below if your
# # file differs.
# # ═══════════════════════════════════════════════════════════════════════════

# BACKTEST_CSV_PATH = os.getenv("BACKTEST_CSV_PATH")
# if not BACKTEST_CSV_PATH or not os.path.exists(BACKTEST_CSV_PATH):
#     log.error("Set BACKTEST_CSV_PATH to a valid 1-minute OHLCV CSV file.")
#     sys.exit(1)

# ADAPTIVE_MODE = os.getenv("ADAPTIVE_MODE", "true").lower() == "true"
# COMMIT_EVERY  = int(os.getenv("BACKTEST_COMMIT_EVERY", "500"))  # batch commits for speed
# SYMBOL        = os.getenv("BACKTEST_SYMBOL", "BTCUSDT")

# log.info("Backtest mode: %s | source=%s", "ADAPTIVE" if ADAPTIVE_MODE else "STATIC BASELINE", BACKTEST_CSV_PATH)

# # ─── Hyperparameters (identical to live pipeline) ─────────────────────────────
# DRIFT_CONFIRMATION_THRESHOLD  = int(os.getenv("DRIFT_CONFIRMATION_THRESHOLD", "3"))
# CUSUM_THRESHOLD               = float(os.getenv("CUSUM_THRESHOLD", "5.0"))
# CUSUM_DRIFT_PARAM             = float(os.getenv("CUSUM_DRIFT_PARAM", "0.5"))
# ISO_WINDOW                    = int(os.getenv("ISO_WINDOW", "200"))
# ISO_MIN_TRAIN                 = int(os.getenv("ISO_MIN_TRAIN", "50"))
# ISO_THRESHOLD                 = float(os.getenv("ISO_THRESHOLD", "-0.6"))
# ISO_CONTAMINATION             = float(os.getenv("ISO_CONTAMINATION", "0.05"))
# HST_THRESHOLD                 = float(os.getenv("HST_THRESHOLD", "0.6"))
# VOL_WINDOW                    = int(os.getenv("VOL_WINDOW", "60"))
# BASELINE_VOL_WINDOW           = int(os.getenv("BASELINE_VOL_WINDOW", "500"))

# CRISIS_ZSCORE_THRESHOLD       = float(os.getenv("CRISIS_ZSCORE_THRESHOLD", "4.0"))
# CRISIS_VOL_RATIO_THRESHOLD    = float(os.getenv("CRISIS_VOL_RATIO_THRESHOLD", "3.0"))
# CRISIS_DRIFT_RATE_THRESHOLD   = float(os.getenv("CRISIS_DRIFT_RATE_THRESHOLD", "0.8"))
# REGIME_ZSCORE_THRESHOLD       = float(os.getenv("REGIME_ZSCORE_THRESHOLD", "2.5"))
# REGIME_VOL_RATIO_THRESHOLD    = float(os.getenv("REGIME_VOL_RATIO_THRESHOLD", "1.5"))
# REGIME_ADWIN_WIDTH_THRESHOLD  = int(os.getenv("REGIME_ADWIN_WIDTH_THRESHOLD", "50"))
# CRISIS_RECOVERY_STABLE_COUNT  = int(os.getenv("CRISIS_RECOVERY_STABLE_COUNT", "3"))
# CRISIS_RECOVERY_VOL_RATIO     = float(os.getenv("CRISIS_RECOVERY_VOL_RATIO", "1.5"))
# INTRADAY_CUSUM_WIDEN_FACTOR   = float(os.getenv("INTRADAY_CUSUM_WIDEN_FACTOR", "1.2"))
# CUSUM_EMA_SPAN                = int(os.getenv("CUSUM_EMA_SPAN", "5"))


# # ─── Database ─────────────────────────────────────────────────────────────────
# conn = psycopg2.connect(
#     host=os.getenv("TIMESCALE_HOST"),
#     port=os.getenv("TIMESCALE_PORT"),
#     user=os.getenv("TIMESCALE_USER"),
#     password=os.getenv("TIMESCALE_PASSWORD"),
#     dbname=os.getenv("TIMESCALE_DB"),
# )
# cur = conn.cursor()

# cur.execute("""
# CREATE TABLE IF NOT EXISTS drift_events (
#     time                  TIMESTAMPTZ NOT NULL,
#     detector              TEXT,
#     price                 DOUBLE PRECISION,
#     message               TEXT,
#     confirmation_count    INT,
#     first_signal_time     TIMESTAMPTZ,
#     adaptation_latency_s  DOUBLE PRECISION,
#     adaptive_mode         BOOLEAN,
#     severity              TEXT,
#     adaptation_strategy   TEXT,
#     vol_ratio             DOUBLE PRECISION,
#     z_score               DOUBLE PRECISION,
#     drift_rate            DOUBLE PRECISION,
#     adwin_width           INT,
#     crisis_mode           BOOLEAN DEFAULT FALSE
# );
# """)

# cur.execute("""
# CREATE TABLE IF NOT EXISTS anomaly_alerts (
#     time               TIMESTAMPTZ NOT NULL,
#     symbol             TEXT,
#     price              DOUBLE PRECISION,
#     detector           TEXT,
#     score              DOUBLE PRECISION,
#     is_anomaly         BOOLEAN,
#     adaptive_mode      BOOLEAN,
#     crisis_mode        BOOLEAN DEFAULT FALSE,
#     severity_context   TEXT
# );
# """)

# for table in ["drift_events", "anomaly_alerts"]:
#     cur.execute(f"""
#         SELECT create_hypertable('{table}', 'time',
#                if_not_exists => TRUE,
#                migrate_data => TRUE);
#     """)

# conn.commit()
# log.info("Database ready")


# # ─── Rolling statistics (identical to live pipeline) ──────────────────────────
# class RollingStats:
#     def __init__(self, short_window=60, long_window=500):
#         self.short = deque(maxlen=short_window)
#         self.long  = deque(maxlen=long_window)
#         self.drift_signals = deque(maxlen=20)

#     def update(self, value, drift_signal: bool):
#         self.short.append(value)
#         self.long.append(value)
#         self.drift_signals.append(1 if drift_signal else 0)

#     @property
#     def rolling_vol(self):
#         return float(np.std(self.short)) if len(self.short) > 1 else 0.0

#     @property
#     def baseline_vol(self):
#         return float(np.std(self.long)) if len(self.long) > 1 else 1e-9

#     @property
#     def rolling_mean(self):
#         return float(np.mean(self.short)) if self.short else 0.0

#     @property
#     def vol_ratio(self):
#         return self.rolling_vol / max(self.baseline_vol, 1e-9)

#     def z_score(self, value):
#         std = self.rolling_vol
#         if std < 1e-9:
#             return 0.0
#         return (value - self.rolling_mean) / std

#     @property
#     def drift_rate(self):
#         if not self.drift_signals:
#             return 0.0
#         return float(np.mean(self.drift_signals))


# def classify_severity(stats: RollingStats, value: float, adwin_width: int) -> str:
#     vr = stats.vol_ratio
#     zs = abs(stats.z_score(value))
#     dr = stats.drift_rate

#     if zs > CRISIS_ZSCORE_THRESHOLD or vr > CRISIS_VOL_RATIO_THRESHOLD or dr > CRISIS_DRIFT_RATE_THRESHOLD:
#         return "CRISIS"
#     if vr > REGIME_VOL_RATIO_THRESHOLD or zs > REGIME_ZSCORE_THRESHOLD or adwin_width < REGIME_ADWIN_WIDTH_THRESHOLD:
#         return "REGIME_SHIFT"
#     return "INTRADAY_NOISE"


# # ─── CUSUM (fixed version — baseline std computed on smoothed series) ─────────
# class CUSUM:
#     def __init__(self, k_multiplier=0.5, h_multiplier=5.0, min_std=1e-6, history_len=100):
#         self.k_multiplier    = k_multiplier
#         self.baseline_h      = h_multiplier
#         self.h_multiplier    = h_multiplier
#         self.min_std         = min_std
#         self.mean            = None
#         self.cusum_pos       = 0.0
#         self.cusum_neg       = 0.0
#         self.history         = deque(maxlen=history_len)
#         self.frozen          = False
#         self.baseline_std    = min_std

#     def __init__(self, k_multiplier=0.5, h_multiplier=5.0, min_std=1e-6, history_len=200, warmup_ticks=200):
#         self.k_multiplier    = k_multiplier
#         self.baseline_h      = h_multiplier
#         self.h_multiplier    = h_multiplier
#         self.min_std         = min_std
#         self.warmup_ticks    = warmup_ticks
#         self.mean            = None
#         self.cusum_pos       = 0.0
#         self.cusum_neg       = 0.0
#         self.history         = deque(maxlen=history_len)
#         self.frozen          = False
#         self.baseline_std    = min_std

#     def set_baseline_std(self, baseline_std: float):
#         self.baseline_std = max(float(baseline_std), self.min_std)

#     def update(self, value):
#         if self.frozen:
#             return True, 99.0

#         self.history.append(value)

#         # Widened warm-up (200 ticks, not 10): a tiny sample is a fragile
#         # anchor for every future deviation in the stream.
#         if self.mean is None and len(self.history) < self.warmup_ticks:
#             return False, 0.0
#         if self.mean is None:
#             self.mean = float(np.mean(self.history))

#         std = self.baseline_std
#         k   = self.k_multiplier * std
#         h   = self.h_multiplier * std

#         deviation      = value - self.mean
#         self.cusum_pos = max(0.0, self.cusum_pos + deviation - k)
#         self.cusum_neg = max(0.0, self.cusum_neg - deviation - k)

#         is_anomaly = self.cusum_pos > h or self.cusum_neg > h

#         raw_score = max(self.cusum_pos, self.cusum_neg)
#         score     = raw_score / max(h, 1e-9)
#         return is_anomaly, score

#     def adapt_intraday(self):
#         # FIX (validated in synthetic benchmark): widen the threshold only.
#         # Do NOT reset cusum_pos/cusum_neg -- SOFT_ADAPT fires on nearly
#         # every confirmed drift that isn't big enough to be REGIME_SHIFT/
#         # CRISIS, so resetting here wipes out accumulation during the very
#         # sustained move CUSUM is trying to detect.
#         self.h_multiplier = self.baseline_h * INTRADAY_CUSUM_WIDEN_FACTOR

#     def adapt_regime(self):
#         if len(self.history) > 10:
#             self.mean         = float(np.mean(self.history))
#             self.cusum_pos    = 0.0
#             self.cusum_neg    = 0.0
#             self.h_multiplier = self.baseline_h

#     def adapt_crisis_freeze(self):
#         self.frozen = True

#     def adapt_crisis_recover(self):
#         self.frozen = False
#         self.adapt_regime()


# class EMA:
#     def __init__(self, span=5):
#         self.alpha = 2.0 / (span + 1.0)
#         self.value = None

#     def update(self, x):
#         if self.value is None:
#             self.value = x
#         else:
#             self.value = self.alpha * x + (1 - self.alpha) * self.value
#         return self.value


# class AdaptiveIsolationForest:
#     def __init__(self, window_size=200, min_train=50, threshold=-0.6, contamination=0.05):
#         self.window        = deque(maxlen=window_size)
#         self.min_train     = min_train
#         self.threshold     = threshold
#         self.contamination = contamination
#         self.model         = None
#         self.trained       = False
#         self.frozen        = False
#         self.train_count   = 0

#     def update(self, value):
#         self.window.append([value])
#         if self.frozen:
#             return True, 1.0
#         if len(self.window) >= self.min_train and not self.trained:
#             self._train()
#         if not self.trained:
#             return False, 0.0
#         score = float(self.model.score_samples([[value]])[0])
#         return score < self.threshold, abs(score)

#     def _train(self):
#         self.model = IsolationForest(
#             contamination=self.contamination, random_state=42, n_estimators=100
#         )
#         self.model.fit(list(self.window))
#         self.trained     = True
#         self.train_count += 1

#     def adapt_intraday(self):
#         pass

#     def adapt_regime(self):
#         if len(self.window) >= self.min_train:
#             self._train()

#     def adapt_crisis_freeze(self):
#         self.frozen = True

#     def adapt_crisis_recover(self):
#         self.frozen = False
#         self.adapt_regime()


# # ─── Load CSV ──────────────────────────────────────────────────────────────────
# log.info("Loading %s ...", BACKTEST_CSV_PATH)
# df = pd.read_csv(BACKTEST_CSV_PATH)

# BINANCE_KLINES_COLUMNS = [
#     "open_time", "open", "high", "low", "close", "volume",
#     "close_time", "quote_volume", "trades",
#     "taker_buy_base", "taker_buy_quote", "ignore"
# ]

# # Raw Binance klines files (data.binance.vision) have NO header row -- the
# # very first data row gets misread by pandas as column names (e.g. a column
# # literally named "19414.02"). Detect this: if every "column name" parses
# # as a number, the file has no real header, so reload with header=None and
# # assign the known Binance klines column order explicitly.
# def _looks_numeric(x):
#     try:
#         float(x)
#         return True
#     except (TypeError, ValueError):
#         return False

# if all(_looks_numeric(c) for c in df.columns) and len(df.columns) == len(BINANCE_KLINES_COLUMNS):
#     log.info("Detected headerless Binance klines format -- reloading with standard column names.")
#     df = pd.read_csv(BACKTEST_CSV_PATH, header=None, names=BINANCE_KLINES_COLUMNS)

# TIME_COL_CANDIDATES  = ["time", "timestamp", "open_time", "date", "Date"]
# PRICE_COL_CANDIDATES = ["close", "price", "Close", "Price"]

# time_col  = next((c for c in TIME_COL_CANDIDATES if c in df.columns), None)
# price_col = next((c for c in PRICE_COL_CANDIDATES if c in df.columns), None)

# if time_col is None or price_col is None:
#     log.error("Could not find time/price columns. Found columns: %s", list(df.columns))
#     log.error("Edit TIME_COL_CANDIDATES / PRICE_COL_CANDIDATES to match your CSV.")
#     sys.exit(1)

# # Binance klines often give open_time in epoch ms — handle both epoch and ISO strings
# if pd.api.types.is_numeric_dtype(df[time_col]):
#     unit = "ms" if df[time_col].iloc[0] > 1e12 else "s"
#     df["_ts"] = pd.to_datetime(df[time_col], unit=unit, utc=True)
# else:
#     df["_ts"] = pd.to_datetime(df[time_col], utc=True)

# df = df[["_ts", price_col]].rename(columns={"_ts": "ts_val", price_col: "price_val"})
# df = df.dropna()
# df = df.sort_values("ts_val").reset_index(drop=True)
# log.info("Loaded %d bars from %s to %s", len(df), df["ts_val"].iloc[0], df["ts_val"].iloc[-1])


# # ─── Initialise detectors ──────────────────────────────────────────────────────
# adwin        = drift.ADWIN()
# page_hinkley = drift.PageHinkley()
# half_space   = anomaly.HalfSpaceTrees(n_trees=10, height=8, window_size=100, seed=42)

# cusum        = CUSUM(k_multiplier=CUSUM_DRIFT_PARAM, h_multiplier=CUSUM_THRESHOLD)
# cusum_ema    = EMA(span=CUSUM_EMA_SPAN)
# iso_forest   = AdaptiveIsolationForest(
#     window_size=ISO_WINDOW, min_train=ISO_MIN_TRAIN,
#     threshold=ISO_THRESHOLD, contamination=ISO_CONTAMINATION
# )

# stats        = RollingStats(short_window=VOL_WINDOW, long_window=BASELINE_VOL_WINDOW)
# cusum_stats  = RollingStats(short_window=VOL_WINDOW, long_window=BASELINE_VOL_WINDOW)

# crisis_state = {"active": False, "onset_time": None, "stable_count": 0}


# def dispatch_adaptation(severity, cusum, iso_forest, crisis_state, now):
#     if severity == "INTRADAY_NOISE":
#         cusum.adapt_intraday()
#         iso_forest.adapt_intraday()
#         return "SOFT_ADAPT"
#     elif severity == "REGIME_SHIFT":
#         cusum.adapt_regime()
#         iso_forest.adapt_regime()
#         return "STANDARD_RECALIBRATION"
#     elif severity == "CRISIS":
#         cusum.adapt_crisis_freeze()
#         iso_forest.adapt_crisis_freeze()
#         crisis_state["active"]       = True
#         crisis_state["onset_time"]   = now
#         crisis_state["stable_count"] = 0
#         return "CRISIS_FREEZE"
#     return "UNKNOWN"


# def check_crisis_recovery(stats, cusum, iso_forest, crisis_state):
#     if not crisis_state["active"]:
#         return False
#     if stats.vol_ratio < CRISIS_RECOVERY_VOL_RATIO:
#         crisis_state["stable_count"] += 1
#     else:
#         crisis_state["stable_count"] = 0
#     if crisis_state["stable_count"] >= CRISIS_RECOVERY_STABLE_COUNT:
#         cusum.adapt_crisis_recover()
#         iso_forest.adapt_crisis_recover()
#         crisis_state["active"]       = False
#         crisis_state["onset_time"]   = None
#         crisis_state["stable_count"] = 0
#         return True
#     return False


# # ─── Replay loop ────────────────────────────────────────────────────────────────
# drift_confirmation_count = 0
# drift_first_signal_time  = None
# total_ticks              = 0
# total_ensemble_anomalies = 0
# total_drift_events       = 0
# prev_price               = None
# inserts_buffer           = []

# # Settling window: after REGIME_SHIFT recalibration, give the freshly
# # recalibrated baseline a short window to bed in before resuming active
# # voting (validated in the synthetic benchmark).
# SETTLING_PERIOD_TICKS = 100
# settling_until = -1

# # Periodic unconditional recalibration safety net -- bounds how far CUSUM
# # can run away if confirmed drift never happens to trigger a reset.
# RECALIBRATION_INTERVAL = 300

# # Empirical threshold calibration for both CUSUM and Isolation Forest,
# # using the real data's own calm early period rather than constants
# # borrowed from live-pipeline tuning. Suppressed (not counted as flags)
# # until each finishes calibrating.
# CALIBRATION_LEN = 150
# iso_calib_scores = []
# iso_calibrated = False
# cusum_calib_scores = []
# cusum_calibrated = False

# n = len(df)
# for i, row in enumerate(df.itertuples(index=False)):
#     now   = row.ts_val.to_pydatetime().replace(tzinfo=timezone.utc)
#     price = float(row.price_val)
#     total_ticks += 1

#     if prev_price is not None and prev_price > 0:
#         price_return = (price - prev_price) / prev_price * 100.0
#     else:
#         price_return = 0.0
#     prev_price = price

#     if crisis_state["active"]:
#         stats.update(price_return, drift_signal=False)
#         recovered = check_crisis_recovery(stats, cusum, iso_forest, crisis_state)
#         if recovered:
#             inserts_buffer.append((
#                 "INSERT INTO drift_events "
#                 "(time, detector, price, message, adaptive_mode, severity, "
#                 " adaptation_strategy, vol_ratio, crisis_mode) "
#                 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
#                 (now, "RECOVERY", price, "Crisis resolved — models retrained",
#                  ADAPTIVE_MODE, "CRISIS_RESOLVED", "POST_CRISIS_RETRAIN",
#                  stats.vol_ratio, False)
#             ))
#     else:
#         stats.update(price_return, drift_signal=False)

#     if ADAPTIVE_MODE and not crisis_state["active"]:
#         adwin_drift = adwin.update(price_return)
#         ph_drift    = page_hinkley.update(price_return)
#         any_drift_signal = adwin_drift or ph_drift

#         if any_drift_signal:
#             stats.update(price_return, drift_signal=True)
#             if drift_confirmation_count == 0:
#                 drift_first_signal_time = now
#             drift_confirmation_count += 1
#         else:
#             drift_confirmation_count = max(0, drift_confirmation_count - 1)
#             if drift_confirmation_count == 0:
#                 drift_first_signal_time = None

#         if drift_confirmation_count >= DRIFT_CONFIRMATION_THRESHOLD:
#             adaptation_latency_s = (
#                 (now - drift_first_signal_time).total_seconds()
#                 if drift_first_signal_time else None
#             )
#             # Cold-start gate (validated in synthetic benchmark): don't
#             # classify severity until the long baseline window has actually
#             # filled. Before that, vol_ratio is computed from a near-empty
#             # baseline_vol and can spike spuriously, wrongly triggering
#             # CRISIS during warm-up and freezing every detector for the
#             # rest of the run.
#             if len(stats.long) < BASELINE_VOL_WINDOW:
#                 drift_confirmation_count = 0
#             else:
#                 adwin_width = getattr(adwin, "width", -1)
#                 severity    = classify_severity(stats, price_return, adwin_width)
#                 strategy    = dispatch_adaptation(severity, cusum, iso_forest, crisis_state, now)
#                 total_drift_events += 1
#                 if severity == "REGIME_SHIFT":
#                     settling_until = total_ticks + SETTLING_PERIOD_TICKS

#                 inserts_buffer.append((
#                     "INSERT INTO drift_events "
#                     "(time, detector, price, message, confirmation_count, "
#                     " first_signal_time, adaptation_latency_s, adaptive_mode, "
#                     " severity, adaptation_strategy, vol_ratio, z_score, "
#                     " drift_rate, adwin_width, crisis_mode) "
#                     "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
#                     (now, "ADWIN+PH", price,
#                      f"Drift confirmed — {severity} — {strategy}",
#                      drift_confirmation_count,
#                      drift_first_signal_time,
#                      adaptation_latency_s,
#                      ADAPTIVE_MODE,
#                      severity,
#                      strategy,
#                      stats.vol_ratio,
#                      stats.z_score(price_return),
#                      stats.drift_rate,
#                      adwin_width,
#                      crisis_state["active"])
#                 ))

#             drift_confirmation_count = 0
#             drift_first_signal_time  = None

#     smoothed_return = cusum_ema.update(price_return)
#     cusum_stats.update(smoothed_return, drift_signal=False)
#     cusum.set_baseline_std(cusum_stats.baseline_vol)
#     cusum_anomaly, cusum_score = cusum.update(smoothed_return)

#     # Empirical calibration for CUSUM (validated in synthetic benchmark):
#     # k/h are theoretically scale-invariant, but EMA smoothing makes the
#     # input autocorrelated, so instantaneous std understates natural
#     # wander. Observe the raw accumulator's 99th-percentile calm behavior
#     # and calibrate h_multiplier from real data instead of a borrowed
#     # constant.
#     if cusum.mean is not None and not cusum_calibrated:
#         raw_accum = max(cusum.cusum_pos, cusum.cusum_neg)
#         cusum_calib_scores.append(raw_accum)
#         cusum_anomaly = False
#         if len(cusum_calib_scores) >= CALIBRATION_LEN:
#             target_h = float(np.percentile(cusum_calib_scores, 99)) * 1.5
#             calibrated_h_multiplier = target_h / max(cusum.baseline_std, 1e-9)
#             cusum.h_multiplier = calibrated_h_multiplier
#             cusum.baseline_h   = calibrated_h_multiplier
#             cusum.cusum_pos    = 0.0
#             cusum.cusum_neg    = 0.0
#             cusum_calibrated   = True
#             log.info("CUSUM calibrated at tick %d: h_multiplier=%.2f (was %.2f)",
#                      total_ticks, calibrated_h_multiplier, CUSUM_THRESHOLD)

#     iso_anomaly, iso_score = iso_forest.update(price)

#     # Same empirical calibration for Isolation Forest's threshold.
#     if iso_forest.trained and not iso_calibrated:
#         raw = float(iso_forest.model.score_samples([[price]])[0])
#         iso_calib_scores.append(raw)
#         iso_anomaly = False
#         if len(iso_calib_scores) >= CALIBRATION_LEN:
#             iso_forest.threshold = float(np.percentile(iso_calib_scores, 1))
#             iso_calibrated = True
#             log.info("ISO calibrated at tick %d: threshold=%.4f (was %.4f)",
#                      total_ticks, iso_forest.threshold, ISO_THRESHOLD)

#     hs_score   = half_space.score_one({"value": price_return})
#     half_space.learn_one({"value": price_return})
#     hs_anomaly = hs_score > HST_THRESHOLD or crisis_state["active"]

#     votes            = sum([cusum_anomaly, iso_anomaly, hs_anomaly])
#     # Settling window: suppress ensemble voting for a short period after a
#     # REGIME_SHIFT recalibration, letting the freshly recalibrated baseline
#     # bed in before resuming active scrutiny.
#     in_settling      = total_ticks <= settling_until
#     ensemble_anomaly = votes >= 2 and not in_settling
#     if ensemble_anomaly:
#         total_ensemble_anomalies += 1

#     # Periodic unconditional recalibration safety net.
#     if not crisis_state["active"] and total_ticks > 0 and total_ticks % RECALIBRATION_INTERVAL == 0:
#         cusum.adapt_regime()
#         iso_forest.adapt_regime()

#     severity_ctx = "CRISIS" if crisis_state["active"] else "NORMAL"

#     for detector, is_anom, score in [
#         ("CUSUM",           cusum_anomaly,    cusum_score),
#         ("IsolationForest", iso_anomaly,      iso_score),
#         ("HalfSpaceTrees",  hs_anomaly,       hs_score),
#         ("Ensemble",        ensemble_anomaly, float(votes)),
#     ]:
#         inserts_buffer.append((
#             "INSERT INTO anomaly_alerts "
#             "(time, symbol, price, detector, score, is_anomaly, "
#             " adaptive_mode, crisis_mode, severity_context) "
#             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
#             (now, SYMBOL, price, detector, float(score),
#              bool(is_anom), ADAPTIVE_MODE,
#              crisis_state["active"], severity_ctx)
#         ))

#     # ── Batch commit every COMMIT_EVERY ticks (backtest speed) ─────────────────
#     if len(inserts_buffer) >= COMMIT_EVERY or i == n - 1:
#         for sql, params in inserts_buffer:
#             cur.execute(sql, params)
#         conn.commit()
#         inserts_buffer = []

#     if total_ticks % 10000 == 0:
#         log.info(
#             "progress %d/%d (%.1f%%) — %s — drift_events=%d ensemble_anomalies=%d (%.2f%%)",
#             total_ticks, n, 100 * total_ticks / n, now.isoformat(),
#             total_drift_events, total_ensemble_anomalies,
#             100 * total_ensemble_anomalies / total_ticks
#         )

# log.info(
#     "BACKTEST COMPLETE — %d bars, %d drift_events, %d ensemble anomalies (%.2f%%)",
#     total_ticks, total_drift_events, total_ensemble_anomalies,
#     100 * total_ensemble_anomalies / max(total_ticks, 1)
# )
# log.info("Query results in TimescaleDB with e.g.:")
# log.info("  SELECT * FROM drift_events WHERE time BETWEEN '2022-01-01' AND '2023-01-01' ORDER BY time;")
# log.info("  SELECT * FROM anomaly_alerts WHERE time BETWEEN '2022-01-01' AND '2023-01-01' AND is_anomaly ORDER BY time;")

import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import timezone
from collections import deque, defaultdict
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
from river import drift, anomaly
from sklearn.ensemble import IsolationForest

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# BACKTEST HARNESS
#
# Replays historical 1-minute bars through the SAME (fixed) detector logic
# as the live Kafka pipeline, and writes to the SAME drift_events /
# anomaly_alerts TimescaleDB tables. Nothing about CUSUM/ADWIN/PageHinkley/
# IsolationForest/HalfSpaceTrees logic differs from the live pipeline —
# only the data source (CSV instead of Kafka) and the timestamp (the bar's
# own historical timestamp instead of datetime.now()).
#
# Because it writes into the same tables, backtest rows and live rows are
# naturally separated by `time` — e.g. filter WHERE time < '2023-01-01'
# for this 2022 backtest vs. live 2026 data. No schema change needed.
#
# CSV INPUT: set BACKTEST_CSV_PATH to a 1-minute OHLCV CSV (e.g. from
# data.binance.vision klines or CryptoDataDownload). The script looks for
# a timestamp column among {time, timestamp, open_time, date} and a price
# column among {close, price, Close}. Adjust COLUMN NAMES below if your
# file differs.
#
# PERF NOTE: DB writes are batched with psycopg2.extras.execute_batch,
# grouped by SQL template, so each flush is a small number of network
# round trips instead of one round trip per row. Doing individual
# cur.execute() calls in a loop (even with a deferred commit()) was
# costing ~1s/tick on a remote TimescaleDB host, since each execute()
# is still its own round trip regardless of when commit() happens.
# ═══════════════════════════════════════════════════════════════════════════

BACKTEST_CSV_PATH = os.getenv("BACKTEST_CSV_PATH")
if not BACKTEST_CSV_PATH or not os.path.exists(BACKTEST_CSV_PATH):
    log.error("Set BACKTEST_CSV_PATH to a valid 1-minute OHLCV CSV file.")
    sys.exit(1)

ADAPTIVE_MODE = os.getenv("ADAPTIVE_MODE", "true").lower() == "true"
COMMIT_EVERY  = int(os.getenv("BACKTEST_COMMIT_EVERY", "50"))  # batch commits for speed
SYMBOL        = os.getenv("BACKTEST_SYMBOL", "BTCUSDT")

log.info("Backtest mode: %s | source=%s", "ADAPTIVE" if ADAPTIVE_MODE else "STATIC BASELINE", BACKTEST_CSV_PATH)

# ─── Hyperparameters (identical to live pipeline) ─────────────────────────────
DRIFT_CONFIRMATION_THRESHOLD  = int(os.getenv("DRIFT_CONFIRMATION_THRESHOLD", "3"))
CUSUM_THRESHOLD               = float(os.getenv("CUSUM_THRESHOLD", "5.0"))
CUSUM_DRIFT_PARAM             = float(os.getenv("CUSUM_DRIFT_PARAM", "0.5"))
ISO_WINDOW                    = int(os.getenv("ISO_WINDOW", "200"))
ISO_MIN_TRAIN                 = int(os.getenv("ISO_MIN_TRAIN", "50"))
ISO_THRESHOLD                 = float(os.getenv("ISO_THRESHOLD", "-0.6"))
ISO_CONTAMINATION             = float(os.getenv("ISO_CONTAMINATION", "0.05"))
HST_THRESHOLD                 = float(os.getenv("HST_THRESHOLD", "0.6"))
VOL_WINDOW                    = int(os.getenv("VOL_WINDOW", "60"))
BASELINE_VOL_WINDOW           = int(os.getenv("BASELINE_VOL_WINDOW", "500"))

CRISIS_ZSCORE_THRESHOLD       = float(os.getenv("CRISIS_ZSCORE_THRESHOLD", "4.0"))
CRISIS_VOL_RATIO_THRESHOLD    = float(os.getenv("CRISIS_VOL_RATIO_THRESHOLD", "3.0"))
CRISIS_DRIFT_RATE_THRESHOLD   = float(os.getenv("CRISIS_DRIFT_RATE_THRESHOLD", "0.8"))
REGIME_ZSCORE_THRESHOLD       = float(os.getenv("REGIME_ZSCORE_THRESHOLD", "2.5"))
REGIME_VOL_RATIO_THRESHOLD    = float(os.getenv("REGIME_VOL_RATIO_THRESHOLD", "1.5"))
REGIME_ADWIN_WIDTH_THRESHOLD  = int(os.getenv("REGIME_ADWIN_WIDTH_THRESHOLD", "50"))
CRISIS_RECOVERY_STABLE_COUNT  = int(os.getenv("CRISIS_RECOVERY_STABLE_COUNT", "3"))
CRISIS_RECOVERY_VOL_RATIO     = float(os.getenv("CRISIS_RECOVERY_VOL_RATIO", "1.5"))
INTRADAY_CUSUM_WIDEN_FACTOR   = float(os.getenv("INTRADAY_CUSUM_WIDEN_FACTOR", "1.2"))
CUSUM_EMA_SPAN                = int(os.getenv("CUSUM_EMA_SPAN", "5"))
HST_CALIB_LEN = 150
hst_calib_scores = []
hst_calibrated = False


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


# ─── Rolling statistics (identical to live pipeline) ──────────────────────────
class RollingStats:
    def __init__(self, short_window=60, long_window=500):
        self.short = deque(maxlen=short_window)
        self.long  = deque(maxlen=long_window)
        self.drift_signals = deque(maxlen=20)

    def update(self, value, drift_signal: bool):
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


def classify_severity(stats: RollingStats, value: float, adwin_width: int) -> str:
    vr = stats.vol_ratio
    zs = abs(stats.z_score(value))
    dr = stats.drift_rate

    if zs > CRISIS_ZSCORE_THRESHOLD or vr > CRISIS_VOL_RATIO_THRESHOLD or dr > CRISIS_DRIFT_RATE_THRESHOLD:
        return "CRISIS"
    if vr > REGIME_VOL_RATIO_THRESHOLD or zs > REGIME_ZSCORE_THRESHOLD or adwin_width < REGIME_ADWIN_WIDTH_THRESHOLD:
        return "REGIME_SHIFT"
    return "INTRADAY_NOISE"


# ─── CUSUM (fixed version — baseline std computed on smoothed series) ─────────
class CUSUM:
    def __init__(self, k_multiplier=0.5, h_multiplier=5.0, min_std=1e-6, history_len=200, warmup_ticks=200):
        self.k_multiplier    = k_multiplier
        self.baseline_h      = h_multiplier
        self.h_multiplier    = h_multiplier
        self.min_std         = min_std
        self.warmup_ticks    = warmup_ticks
        self.mean            = None
        self.cusum_pos       = 0.0
        self.cusum_neg       = 0.0
        self.history         = deque(maxlen=history_len)
        self.frozen          = False
        self.baseline_std    = min_std

    def set_baseline_std(self, baseline_std: float):
        self.baseline_std = max(float(baseline_std), self.min_std)

    def update(self, value):
        if self.frozen:
            return True, 99.0

        self.history.append(value)

        # Widened warm-up (200 ticks, not 10): a tiny sample is a fragile
        # anchor for every future deviation in the stream.
        if self.mean is None and len(self.history) < self.warmup_ticks:
            return False, 0.0
        if self.mean is None:
            self.mean = float(np.mean(self.history))

        std = self.baseline_std
        k   = self.k_multiplier * std
        h   = self.h_multiplier * std

        deviation      = value - self.mean
        self.cusum_pos = max(0.0, self.cusum_pos + deviation - k)
        self.cusum_neg = max(0.0, self.cusum_neg - deviation - k)

        is_anomaly = self.cusum_pos > h or self.cusum_neg > h

        raw_score = max(self.cusum_pos, self.cusum_neg)
        score     = raw_score / max(h, 1e-9)
        return is_anomaly, score

    def adapt_intraday(self):
        # FIX (validated in synthetic benchmark): widen the threshold only.
        # Do NOT reset cusum_pos/cusum_neg -- SOFT_ADAPT fires on nearly
        # every confirmed drift that isn't big enough to be REGIME_SHIFT/
        # CRISIS, so resetting here wipes out accumulation during the very
        # sustained move CUSUM is trying to detect.
        self.h_multiplier = self.baseline_h * INTRADAY_CUSUM_WIDEN_FACTOR

    def adapt_regime(self):
        if len(self.history) > 10:
            self.mean         = float(np.mean(self.history))
            self.cusum_pos    = 0.0
            self.cusum_neg    = 0.0
            self.h_multiplier = self.baseline_h
    # def adapt_regime(self):
    #     if len(self.window) >= self.min_train:
    #         self._train()
    #         # re-derive threshold from the freshly retrained model's own
    #         # score distribution on its current window, so the cutoff
    #         # tracks the data instead of staying pinned to the original
    #         # calm-period calibration
    #         scores = self.model.score_samples(list(self.window))
    #         self.threshold = float(np.percentile(scores, 1))

    def adapt_crisis_freeze(self):
        self.frozen = True

    def adapt_crisis_recover(self):
        self.frozen = False
        self.adapt_regime()


class EMA:
    def __init__(self, span=5):
        self.alpha = 2.0 / (span + 1.0)
        self.value = None

    def update(self, x):
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1 - self.alpha) * self.value
        return self.value


class AdaptiveIsolationForest:
    def __init__(self, window_size=200, min_train=50, threshold=-0.6, contamination=0.05):
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
            contamination=self.contamination, random_state=42, n_estimators=100
        )
        self.model.fit(list(self.window))
        self.trained     = True
        self.train_count += 1

    def adapt_intraday(self):
        pass

    def adapt_regime(self):
        if len(self.window) >= self.min_train:
            self._train()

    def adapt_crisis_freeze(self):
        self.frozen = True

    def adapt_crisis_recover(self):
        self.frozen = False
        self.adapt_regime()


# ─── Load CSV ──────────────────────────────────────────────────────────────────
log.info("Loading %s ...", BACKTEST_CSV_PATH)
df = pd.read_csv(BACKTEST_CSV_PATH)

BINANCE_KLINES_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore"
]

# Raw Binance klines files (data.binance.vision) have NO header row -- the
# very first data row gets misread by pandas as column names (e.g. a column
# literally named "19414.02"). Detect this: if every "column name" parses
# as a number, the file has no real header, so reload with header=None and
# assign the known Binance klines column order explicitly.
# def _looks_numeric(x):
#     try:
#         float(x)
#         return True
#     except (TypeError, ValueError):
#         return False

def _looks_numeric(x):
    """Check if a string looks like a number, handling scientific notation."""
    try:
        float(x)
        return True
    except (TypeError, ValueError):
        # Lenient check: if it starts with a digit or contains 'E'/'e', treat as numeric
        return isinstance(x, str) and (x[0].isdigit() or 'e' in x.lower())

if all(_looks_numeric(c) for c in df.columns) and len(df.columns) == len(BINANCE_KLINES_COLUMNS):
    log.info("Detected headerless Binance klines format -- reloading with standard column names.")
    df = pd.read_csv(BACKTEST_CSV_PATH, header=None, names=BINANCE_KLINES_COLUMNS)

if all(_looks_numeric(c) for c in df.columns) and len(df.columns) == len(BINANCE_KLINES_COLUMNS):
    log.info("Detected headerless Binance klines format -- reloading with standard column names.")
    df = pd.read_csv(BACKTEST_CSV_PATH, header=None, names=BINANCE_KLINES_COLUMNS)

TIME_COL_CANDIDATES  = ["time", "timestamp", "open_time", "date", "Date"]
PRICE_COL_CANDIDATES = ["close", "price", "Close", "Price"]

time_col  = next((c for c in TIME_COL_CANDIDATES if c in df.columns), None)
price_col = next((c for c in PRICE_COL_CANDIDATES if c in df.columns), None)

if time_col is None or price_col is None:
    log.error("Could not find time/price columns. Found columns: %s", list(df.columns))
    log.error("Edit TIME_COL_CANDIDATES / PRICE_COL_CANDIDATES to match your CSV.")
    sys.exit(1)

# Binance klines often give open_time in epoch ms — handle both epoch and ISO strings
if pd.api.types.is_numeric_dtype(df[time_col]):
    unit = "ms" if df[time_col].iloc[0] > 1e12 else "s"
    df["_ts"] = pd.to_datetime(df[time_col], unit=unit, utc=True)
else:
    df["_ts"] = pd.to_datetime(df[time_col], utc=True)

df = df[["_ts", price_col]].rename(columns={"_ts": "ts_val", price_col: "price_val"})
df = df.dropna()
df = df.sort_values("ts_val").reset_index(drop=True)
log.info("Loaded %d bars from %s to %s", len(df), df["ts_val"].iloc[0], df["ts_val"].iloc[-1])


# ─── Initialise detectors ──────────────────────────────────────────────────────
adwin        = drift.ADWIN()
page_hinkley = drift.PageHinkley()
half_space   = anomaly.HalfSpaceTrees(n_trees=10, height=8, window_size=100, seed=42)

cusum        = CUSUM(k_multiplier=CUSUM_DRIFT_PARAM, h_multiplier=CUSUM_THRESHOLD)
cusum_ema    = EMA(span=CUSUM_EMA_SPAN)
iso_forest   = AdaptiveIsolationForest(
    window_size=ISO_WINDOW, min_train=ISO_MIN_TRAIN,
    threshold=ISO_THRESHOLD, contamination=ISO_CONTAMINATION
)

stats        = RollingStats(short_window=VOL_WINDOW, long_window=BASELINE_VOL_WINDOW)
cusum_stats  = RollingStats(short_window=VOL_WINDOW, long_window=BASELINE_VOL_WINDOW)

crisis_state = {"active": False, "onset_time": None, "stable_count": 0}


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
        cusum.adapt_crisis_recover()
        iso_forest.adapt_crisis_recover()
        crisis_state["active"]       = False
        crisis_state["onset_time"]   = None
        crisis_state["stable_count"] = 0
        return True
    return False


# def flush_inserts(cur, conn, inserts_buffer):
#     """
#     Batch-write everything accumulated in inserts_buffer using
#     psycopg2.extras.execute_batch, grouped by SQL template. This turns
#     N single-row round trips into ~len(distinct SQL templates) batched
#     round trips per flush, instead of one execute() per row (which was
#     the actual bottleneck -- deferring commit() alone does not batch
#     the network round trips, only the transaction boundary).
#     """
#     if not inserts_buffer:
#         return
#     grouped = defaultdict(list)
#     for sql, params in inserts_buffer:
#         grouped[sql].append(params)
#     for sql, param_list in grouped.items():
#         execute_batch(cur, sql, param_list, page_size=500)
#     conn.commit()
def flush_inserts(cur, conn, inserts_buffer):
    """
    Single-insert fallback: write one row at a time instead of batching.
    Slower but won't crash the DB.
    """
    if not inserts_buffer:
        return
    
    for sql, params in inserts_buffer:
        try:
            cur.execute(sql, params)
        except Exception as e:
            log.error(f"Single insert failed: {e}")
            log.error(f"SQL: {sql}")
            log.error(f"Params: {params}")
            raise
    
    conn.commit()


# ─── Replay loop ────────────────────────────────────────────────────────────────
drift_confirmation_count = 0
drift_first_signal_time  = None
total_ticks              = 0
total_ensemble_anomalies = 0
total_drift_events       = 0
prev_price               = None
inserts_buffer           = []

# Settling window: after REGIME_SHIFT recalibration, give the freshly
# recalibrated baseline a short window to bed in before resuming active
# voting (validated in the synthetic benchmark).
SETTLING_PERIOD_TICKS = 100
settling_until = -1

# Periodic unconditional recalibration safety net -- bounds how far CUSUM
# can run away if confirmed drift never happens to trigger a reset.
RECALIBRATION_INTERVAL = 300

# Empirical threshold calibration for both CUSUM and Isolation Forest,
# using the real data's own calm early period rather than constants
# borrowed from live-pipeline tuning. Suppressed (not counted as flags)
# until each finishes calibrating.
CALIBRATION_LEN = 150
iso_calib_scores = []
iso_calibrated = False
cusum_calib_scores = []
cusum_calibrated = False

n = len(df)
for i, row in enumerate(df.itertuples(index=False)):
    now   = row.ts_val.to_pydatetime().replace(tzinfo=timezone.utc)
    price = float(row.price_val)
    total_ticks += 1

    if prev_price is not None and prev_price > 0:
        price_return = (price - prev_price) / prev_price * 100.0
    else:
        price_return = 0.0
    prev_price = price

    if crisis_state["active"]:
        stats.update(price_return, drift_signal=False)
        recovered = check_crisis_recovery(stats, cusum, iso_forest, crisis_state)
        if recovered:
            inserts_buffer.append((
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

    if ADAPTIVE_MODE and not crisis_state["active"]:
        adwin_drift = adwin.update(price_return)
        ph_drift    = page_hinkley.update(price_return)
        any_drift_signal = adwin_drift or ph_drift

        if any_drift_signal:
            stats.update(price_return, drift_signal=True)
            if drift_confirmation_count == 0:
                drift_first_signal_time = now
            drift_confirmation_count += 1
        else:
            drift_confirmation_count = max(0, drift_confirmation_count - 1)
            if drift_confirmation_count == 0:
                drift_first_signal_time = None

        if drift_confirmation_count >= DRIFT_CONFIRMATION_THRESHOLD:
            adaptation_latency_s = (
                (now - drift_first_signal_time).total_seconds()
                if drift_first_signal_time else None
            )
            # Cold-start gate (validated in synthetic benchmark): don't
            # classify severity until the long baseline window has actually
            # filled. Before that, vol_ratio is computed from a near-empty
            # baseline_vol and can spike spuriously, wrongly triggering
            # CRISIS during warm-up and freezing every detector for the
            # rest of the run.
            if len(stats.long) < BASELINE_VOL_WINDOW:
                drift_confirmation_count = 0
            else:
                adwin_width = getattr(adwin, "width", -1)
                severity    = classify_severity(stats, price_return, adwin_width)
                strategy    = dispatch_adaptation(severity, cusum, iso_forest, crisis_state, now)
                total_drift_events += 1
                if severity == "REGIME_SHIFT":
                    settling_until = total_ticks + SETTLING_PERIOD_TICKS

                inserts_buffer.append((
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

    smoothed_return = cusum_ema.update(price_return)
    cusum_stats.update(smoothed_return, drift_signal=False)
    cusum.set_baseline_std(cusum_stats.baseline_vol)
    cusum_anomaly, cusum_score = cusum.update(smoothed_return)

    # Empirical calibration for CUSUM (validated in synthetic benchmark):
    # k/h are theoretically scale-invariant, but EMA smoothing makes the
    # input autocorrelated, so instantaneous std understates natural
    # wander. Observe the raw accumulator's 99th-percentile calm behavior
    # and calibrate h_multiplier from real data instead of a borrowed
    # constant.
    if cusum.mean is not None and not cusum_calibrated:
        raw_accum = max(cusum.cusum_pos, cusum.cusum_neg)
        cusum_calib_scores.append(raw_accum)
        cusum_anomaly = False
        if len(cusum_calib_scores) >= CALIBRATION_LEN:
            target_h = float(np.percentile(cusum_calib_scores, 99)) * 1.5
            calibrated_h_multiplier = target_h / max(cusum.baseline_std, 1e-9)
            cusum.h_multiplier = calibrated_h_multiplier
            cusum.baseline_h   = calibrated_h_multiplier
            cusum.cusum_pos    = 0.0
            cusum.cusum_neg    = 0.0
            cusum_calibrated   = True
            log.info("CUSUM calibrated at tick %d: h_multiplier=%.2f (was %.2f)",
                     total_ticks, calibrated_h_multiplier, CUSUM_THRESHOLD)

    iso_anomaly, iso_score = iso_forest.update(price)

    # Same empirical calibration for Isolation Forest's threshold.
    if iso_forest.trained and not iso_calibrated:
        raw = float(iso_forest.model.score_samples([[price]])[0])
        iso_calib_scores.append(raw)
        iso_anomaly = False
        if len(iso_calib_scores) >= CALIBRATION_LEN:
            iso_forest.threshold = float(np.percentile(iso_calib_scores, 1))
            iso_calibrated = True
            log.info("ISO calibrated at tick %d: threshold=%.4f (was %.4f)",
                     total_ticks, iso_forest.threshold, ISO_THRESHOLD)

    hs_score   = half_space.score_one({"value": price_return})
    half_space.learn_one({"value": price_return})
    # hs_anomaly = hs_score > HST_THRESHOLD or crisis_state["active"]
    if not hst_calibrated:
        hst_calib_scores.append(hs_score)
        hs_anomaly = False
        if len(hst_calib_scores) >= HST_CALIB_LEN:
            HST_THRESHOLD = float(np.percentile(hst_calib_scores, 99))
            hst_calibrated = True
            log.info("HST calibrated at tick %d: threshold=%.4f (was 0.6000)",
                    total_ticks, HST_THRESHOLD)
    else:
        hs_anomaly = hs_score > HST_THRESHOLD or crisis_state["active"]
    votes            = sum([cusum_anomaly, iso_anomaly, hs_anomaly])
    # Settling window: suppress ensemble voting for a short period after a
    # REGIME_SHIFT recalibration, letting the freshly recalibrated baseline
    # bed in before resuming active scrutiny.
    in_settling      = total_ticks <= settling_until
    ensemble_anomaly = votes >= 2 and not in_settling
    if ensemble_anomaly:
        total_ensemble_anomalies += 1

    # Periodic unconditional recalibration safety net.
    if not crisis_state["active"] and total_ticks > 0 and total_ticks % RECALIBRATION_INTERVAL == 0:
        cusum.adapt_regime()
        iso_forest.adapt_regime()

    severity_ctx = "CRISIS" if crisis_state["active"] else "NORMAL"

    for detector, is_anom, score in [
        ("CUSUM",           cusum_anomaly,    cusum_score),
        ("IsolationForest", iso_anomaly,      iso_score),
        ("HalfSpaceTrees",  hs_anomaly,       hs_score),
        ("Ensemble",        ensemble_anomaly, float(votes)),
    ]:
        inserts_buffer.append((
            "INSERT INTO anomaly_alerts "
            "(time, symbol, price, detector, score, is_anomaly, "
            " adaptive_mode, crisis_mode, severity_context) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (now, SYMBOL, price, detector, float(score),
             bool(is_anom), ADAPTIVE_MODE,
             crisis_state["active"], severity_ctx)
        ))

    # ── Batch commit every COMMIT_EVERY ticks (backtest speed) ─────────────────
    if len(inserts_buffer) >= COMMIT_EVERY or i == n - 1:
        flush_inserts(cur, conn, inserts_buffer)
        inserts_buffer = []

    if total_ticks % 10000 == 0:
        log.info(
            "progress %d/%d (%.1f%%) — %s — drift_events=%d ensemble_anomalies=%d (%.2f%%)",
            total_ticks, n, 100 * total_ticks / n, now.isoformat(),
            total_drift_events, total_ensemble_anomalies,
            100 * total_ensemble_anomalies / total_ticks
        )

log.info(
    "BACKTEST COMPLETE — %d bars, %d drift_events, %d ensemble anomalies (%.2f%%)",
    total_ticks, total_drift_events, total_ensemble_anomalies,
    100 * total_ensemble_anomalies / max(total_ticks, 1)
)
log.info("Query results in TimescaleDB with e.g.:")
log.info("  SELECT * FROM drift_events WHERE time BETWEEN '2022-01-01' AND '2023-01-01' ORDER BY time;")
log.info("  SELECT * FROM anomaly_alerts WHERE time BETWEEN '2022-01-01' AND '2023-01-01' AND is_anomaly ORDER BY time;")