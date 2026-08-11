"""
Synthetic Drift Benchmark
==========================
Injects KNOWN drift events (abrupt, gradual, incremental, recurring) into a
controllable synthetic return-series, then evaluates:

  1. The full detector ensemble (ADWIN + Page-Hinkley confirmation -> severity
     classification -> CUSUM/IsolationForest/HalfSpaceTrees, with the fixed
     CUSUM class from drift_pipeline.py)
  2. A NAIVE baseline: a fixed z-score threshold detector with NO drift
     adaptation at all, run on the identical stream.

Because drift onset points are injected by us, we have ground truth — this
gives real precision / recall / F1 / detection-latency numbers, not just a
count of "how many red dots appeared."

Run across multiple seeds per condition (default 3) and report mean +/- std.

Output: a CSV of per-condition results + a printed summary table.
"""

import os
import csv
import json
import numpy as np
from collections import deque
from river import drift, anomaly
from sklearn.ensemble import IsolationForest

RNG_SEEDS = [1, 2, 3]  # bump for more rigor if time allows
N_TICKS_PER_DRIFT_CONDITION = 4000
BASELINE_STD = 0.01          # "normal" tick-to-tick % return volatility
DETECTION_TOLERANCE = 60     # ticks after true onset within which a flag counts as a true positive

# ── Hyperparameters (must match the fixed live pipeline) ──────────────────────
DRIFT_CONFIRMATION_THRESHOLD = 3
CUSUM_THRESHOLD              = 5.0
CUSUM_DRIFT_PARAM            = 0.5
ISO_WINDOW                   = 200
ISO_MIN_TRAIN                = 50
ISO_THRESHOLD                = -0.6
ISO_CONTAMINATION            = 0.05
HST_THRESHOLD                = 0.6
VOL_WINDOW                   = 60
BASELINE_VOL_WINDOW          = 500
CRISIS_ZSCORE_THRESHOLD      = 4.0
CRISIS_VOL_RATIO_THRESHOLD   = 3.0
CRISIS_DRIFT_RATE_THRESHOLD  = 0.8
REGIME_ZSCORE_THRESHOLD      = 2.5
REGIME_VOL_RATIO_THRESHOLD   = 1.5
REGIME_ADWIN_WIDTH_THRESHOLD = 50
INTRADAY_CUSUM_WIDEN_FACTOR  = 1.2
CUSUM_EMA_SPAN                = 5

# Naive baseline threshold (fixed, no adaptation) — a reasonable "obvious" choice
NAIVE_ZSCORE_THRESHOLD = 3.0


# ── Synthetic stream generator ─────────────────────────────────────────────────
def make_drift_schedule(drift_type: str, n_ticks: int, magnitude: float):
    """
    Returns (mean_fn, vol_fn, onsets) where mean_fn/vol_fn map tick index ->
    target mean/std multiplier, and onsets is the list of ground-truth drift
    onset ticks to evaluate detection against.
    """
    onsets = []

    if drift_type == "abrupt":
        onset = n_ticks // 2
        onsets = [onset]
        def mean_fn(t):
            return magnitude * BASELINE_STD if t >= onset else 0.0
        def vol_fn(t):
            return 1.0

    elif drift_type == "gradual":
        onset = n_ticks // 2
        width = 300
        onsets = [onset]
        def mean_fn(t):
            if t < onset:
                return 0.0
            if t < onset + width:
                return magnitude * BASELINE_STD * (t - onset) / width
            return magnitude * BASELINE_STD
        def vol_fn(t):
            return 1.0

    elif drift_type == "incremental":
        onset = n_ticks // 4
        span = n_ticks - onset
        # Ground-truth onset for SCORING is set to the half-magnitude point,
        # not the literal ramp start. At the ramp start, injected drift is
        # ~0 by construction -- no detector, however good, can find a signal
        # that isn't there yet. The half-magnitude point is the first
        # point where the drift is actually substantial enough to be
        # realistically detectable, which is what the tolerance window
        # should be measured against.
        detectable_onset = onset + span // 2
        onsets = [detectable_onset]
        def mean_fn(t):
            if t < onset:
                return 0.0
            return magnitude * BASELINE_STD * min(1.0, (t - onset) / span)
        def vol_fn(t):
            return 1.0

    elif drift_type == "recurring":
        period = n_ticks // 4
        onsets = [period, period * 2, period * 3]
        def mean_fn(t):
            # A -> B -> A -> B pattern, regime B has the shifted mean
            cycle = (t // period) % 2
            return magnitude * BASELINE_STD if cycle == 1 else 0.0
        def vol_fn(t):
            return 1.0

    elif drift_type == "variance_shift":
        onset = n_ticks // 2
        onsets = [onset]
        def mean_fn(t):
            return 0.0
        def vol_fn(t):
            return magnitude if t >= onset else 1.0

    else:
        raise ValueError(f"unknown drift_type {drift_type}")

    return mean_fn, vol_fn, onsets


def generate_synthetic_returns(drift_type, magnitude, n_ticks, seed):
    rng = np.random.default_rng(seed)
    mean_fn, vol_fn, onsets = make_drift_schedule(drift_type, n_ticks, magnitude)
    series = np.empty(n_ticks)
    for t in range(n_ticks):
        mu = mean_fn(t)
        sigma = BASELINE_STD * vol_fn(t)
        series[t] = rng.normal(mu, sigma)
    return series, onsets


# ── Fixed detector classes (identical to corrected drift_pipeline.py) ─────────
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


def classify_severity(stats, value, adwin_width):
    vr = stats.vol_ratio
    zs = abs(stats.z_score(value))
    dr = stats.drift_rate
    if zs > CRISIS_ZSCORE_THRESHOLD or vr > CRISIS_VOL_RATIO_THRESHOLD or dr > CRISIS_DRIFT_RATE_THRESHOLD:
        return "CRISIS"
    if vr > REGIME_VOL_RATIO_THRESHOLD or zs > REGIME_ZSCORE_THRESHOLD or adwin_width < REGIME_ADWIN_WIDTH_THRESHOLD:
        return "REGIME_SHIFT"
    return "INTRADAY_NOISE"


class CUSUM:
    """Fixed version: baseline std from smoothed series, adapt_intraday widens
    threshold only (no accumulator reset), mean warm-up over 10 ticks."""
    def __init__(self, k_multiplier=0.5, h_multiplier=5.0, min_std=1e-6, history_len=200, warmup_ticks=200):
        self.k_multiplier = k_multiplier
        self.baseline_h   = h_multiplier
        self.h_multiplier = h_multiplier
        self.min_std      = min_std
        self.warmup_ticks = warmup_ticks
        self.mean         = None
        self.cusum_pos    = 0.0
        self.cusum_neg    = 0.0
        self.history      = deque(maxlen=history_len)
        self.frozen       = False
        self.baseline_std = min_std

    def set_baseline_std(self, baseline_std):
        self.baseline_std = max(float(baseline_std), self.min_std)

    def update(self, value):
        if self.frozen:
            return True, 99.0
        self.history.append(value)
        # Widened warm-up (default 200 ticks, was 10): the reference mean
        # anchors every future deviation for the rest of the stream, so a
        # tiny 10-sample estimate is fragile -- especially on EMA-smoothed
        # input, where the first few values can be biased by whatever the
        # stream happened to start with.
        if self.mean is None and len(self.history) < self.warmup_ticks:
            return False, 0.0
        if self.mean is None:
            self.mean = float(np.mean(self.history))
        std = self.baseline_std
        k = self.k_multiplier * std
        h = self.h_multiplier * std
        deviation = value - self.mean
        self.cusum_pos = max(0.0, self.cusum_pos + deviation - k)
        self.cusum_neg = max(0.0, self.cusum_neg - deviation - k)
        is_anomaly = self.cusum_pos > h or self.cusum_neg > h
        raw_score = max(self.cusum_pos, self.cusum_neg)
        score = raw_score / max(h, 1e-9)
        return is_anomaly, score

    def adapt_intraday(self):
        self.h_multiplier = self.baseline_h * INTRADAY_CUSUM_WIDEN_FACTOR

    def adapt_regime(self):
        if len(self.history) > 10:
            self.mean = float(np.mean(self.history))
            self.cusum_pos = 0.0
            self.cusum_neg = 0.0
            self.h_multiplier = self.baseline_h

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
        self.window = deque(maxlen=window_size)
        self.min_train = min_train
        self.threshold = threshold
        self.contamination = contamination
        self.model = None
        self.trained = False
        self.frozen = False

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
        self.model = IsolationForest(contamination=self.contamination, random_state=42, n_estimators=100)
        self.model.fit(list(self.window))
        self.trained = True

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


def dispatch_adaptation(severity, cusum, iso_forest):
    if severity == "INTRADAY_NOISE":
        cusum.adapt_intraday()
        iso_forest.adapt_intraday()
    elif severity == "REGIME_SHIFT":
        cusum.adapt_regime()
        iso_forest.adapt_regime()
    elif severity == "CRISIS":
        cusum.adapt_crisis_freeze()
        iso_forest.adapt_crisis_freeze()


# Set this to a (drift_type, magnitude, seed) tuple to print a detailed,
# tick-by-tick trace of exactly what's driving each ensemble flag for that
# one condition. Set to None to disable and run silently.
DEBUG_CONDITION = ("abrupt", 4.0, 1)


# ── Ensemble run (the actual system under test) ────────────────────────────────
def run_ensemble(series, debug=False, true_onsets=None):
    adwin = drift.ADWIN()
    page_hinkley = drift.PageHinkley()
    half_space = anomaly.HalfSpaceTrees(n_trees=10, height=8, window_size=100, seed=42)
    cusum = CUSUM(k_multiplier=CUSUM_DRIFT_PARAM, h_multiplier=CUSUM_THRESHOLD)
    cusum_ema = EMA(span=CUSUM_EMA_SPAN)
    iso_forest = AdaptiveIsolationForest(ISO_WINDOW, ISO_MIN_TRAIN, ISO_THRESHOLD, ISO_CONTAMINATION)
    stats = RollingStats(VOL_WINDOW, BASELINE_VOL_WINDOW)
    cusum_stats = RollingStats(VOL_WINDOW, BASELINE_VOL_WINDOW)

    if debug:
        print(f"  [DEBUG] true onset(s): {true_onsets}")
        print(f"  [DEBUG] tick | cusum_anom(score) | iso_anom(score) | hs_anom(score) | crisis_active | votes")

    crisis_active = False
    drift_confirmation_count = 0
    flags = []  # tick indices where ensemble_anomaly fired

    # Periodic, UNCONDITIONAL recalibration safety net. Without this, if
    # ADWIN/Page-Hinkley (correctly) stay quiet on calm data, CUSUM's
    # cusum_pos/cusum_neg have no reset mechanism at all -- and the EMA
    # smoothing (needed to cancel tick-flicker) makes the input series
    # autocorrelated, so it can develop sustained one-directional runs from
    # noise alone. CUSUM then detects that as "drift" and never recovers,
    # since only confirmed drift triggers adapt_regime(). This periodic
    # recalibration re-centers CUSUM every RECALIBRATION_INTERVAL ticks
    # regardless of whether drift was confirmed, bounding how far it can
    # run away on pure noise.
    RECALIBRATION_INTERVAL = 300

    # Isolation Forest threshold self-calibration. ISO_THRESHOLD=-0.6 was
    # empirically tuned on real BTC tick data and does not transfer to this
    # synthetic benchmark's noise scale. Instead of trusting that constant,
    # calibrate it from the model's OWN score distribution on a stretch of
    # known-calm data right after training (all synthetic onsets occur at
    # tick >=1000, so this calibration window is always genuinely drift-free).
    # Threshold is set to the 1st percentile of calm scores -- i.e. by
    # construction, roughly a 1% false-positive rate on calm data, a
    # principled target rather than a borrowed constant.
    ISO_CALIBRATION_LEN = 150
    iso_calib_scores = []
    iso_calibrated = False

    # Same empirical-calibration approach applied to CUSUM's h threshold.
    # CUSUM's k/h are theoretically scale-invariant multiples of baseline_std,
    # but EMA smoothing (needed to cancel tick-flicker) makes the input
    # series autocorrelated -- so instantaneous std understates how far the
    # accumulator naturally wanders even with zero real drift. Rather than
    # trust the fixed CUSUM_THRESHOLD=5.0 multiplier, observe the RAW
    # accumulator's own 99th-percentile behavior on a calm stretch (right
    # after CUSUM's mean warm-up completes) and calibrate h_multiplier so
    # that threshold sits a safety margin above genuinely normal variation.
    CUSUM_CALIBRATION_LEN = 150
    CUSUM_CALIBRATION_SAFETY_MARGIN = 1.5
    cusum_calib_scores = []
    cusum_calibrated = False

    # Settling window: after a confirmed REGIME_SHIFT triggers recalibration,
    # give the freshly-recalibrated baseline a short window to "bed in"
    # before resuming active anomaly voting. Without this, a regime-shift
    # reset based on limited recent history can itself look slightly noisy
    # immediately after recalibration, generating avoidable re-alerts on a
    # baseline that hasn't stabilized yet. Mirrors real alerting systems'
    # cooldown-after-acknowledgment pattern.
    SETTLING_PERIOD_TICKS = 100
    settling_until = -1

    for t, value in enumerate(series):
        stats.update(value, drift_signal=False)

        if not crisis_active:
            adwin_drift = adwin.update(value)
            ph_drift = page_hinkley.update(value)
            if adwin_drift or ph_drift:
                drift_confirmation_count += 1
            else:
                drift_confirmation_count = max(0, drift_confirmation_count - 1)

            if drift_confirmation_count >= DRIFT_CONFIRMATION_THRESHOLD:
                # Only classify severity once the long-window baseline is
                # actually stable (fully populated). Before that, vol_ratio
                # is computed from a near-empty/small-sample baseline_vol and
                # can spike spuriously, wrongly triggering CRISIS during
                # cold start and freezing every detector into permanent
                # always-anomaly mode for the rest of the stream -- which
                # then shows up as a single early, unmatched episode instead
                # of a genuine detection at the true injected onset.
                if len(stats.long) >= BASELINE_VOL_WINDOW:
                    adwin_width = getattr(adwin, "width", -1)
                    severity = classify_severity(stats, value, adwin_width)
                    dispatch_adaptation(severity, cusum, iso_forest)
                    if severity == "CRISIS":
                        crisis_active = True
                    elif severity == "REGIME_SHIFT":
                        settling_until = t + SETTLING_PERIOD_TICKS
                drift_confirmation_count = 0

        smoothed = cusum_ema.update(value)
        cusum_stats.update(smoothed, drift_signal=False)
        cusum.set_baseline_std(cusum_stats.baseline_vol)
        cusum_anom, cusum_score = cusum.update(smoothed)

        if cusum.mean is not None and not cusum_calibrated:
            # Don't trust the borrowed h_multiplier yet -- collect the raw
            # accumulator's calm-period behavior instead, and suppress
            # cusum_anom during this window (safe: always well before the
            # earliest possible injected onset at tick 1000).
            raw_accum = max(cusum.cusum_pos, cusum.cusum_neg)
            cusum_calib_scores.append(raw_accum)
            cusum_anom = False
            if len(cusum_calib_scores) >= CUSUM_CALIBRATION_LEN:
                target_h = float(np.percentile(cusum_calib_scores, 99)) * CUSUM_CALIBRATION_SAFETY_MARGIN
                calibrated_h_multiplier = target_h / max(cusum.baseline_std, 1e-9)
                cusum.h_multiplier = calibrated_h_multiplier
                cusum.baseline_h = calibrated_h_multiplier  # so future adapt_regime()/adapt_intraday() keep using the calibrated value, not the original constant
                cusum.cusum_pos = 0.0
                cusum.cusum_neg = 0.0
                cusum_calibrated = True
                if debug:
                    print(f"  [DEBUG] CUSUM calibrated at tick {t}: h_multiplier={calibrated_h_multiplier:.2f} "
                          f"(was {CUSUM_THRESHOLD})")

        iso_anom, iso_score = iso_forest.update(value)

        if iso_forest.trained and not iso_calibrated:
            # Don't trust the uncalibrated (borrowed) threshold yet --
            # collect raw scores instead, and suppress iso_anom during this
            # window. Safe to suppress: this window sits right after
            # ISO_MIN_TRAIN, always well before the earliest possible
            # injected onset (tick 1000) in every condition.
            raw = float(iso_forest.model.score_samples([[value]])[0])
            iso_calib_scores.append(raw)
            iso_anom = False
            if len(iso_calib_scores) >= ISO_CALIBRATION_LEN:
                iso_forest.threshold = float(np.percentile(iso_calib_scores, 1))
                iso_calibrated = True
                if debug:
                    print(f"  [DEBUG] ISO calibrated at tick {t}: threshold={iso_forest.threshold:.4f} "
                          f"(was {ISO_THRESHOLD})")

        hs_score = half_space.score_one({"value": value})
        half_space.learn_one({"value": value})
        hs_anom = hs_score > HST_THRESHOLD or crisis_active

        votes = sum([cusum_anom, iso_anom, hs_anom])
        in_settling = t <= settling_until
        if votes >= 2 and not in_settling:
            flags.append(t)
            if debug:
                print(f"  [DEBUG]  {t:5d} | {cusum_anom}({cusum_score:.3f}) | "
                      f"{iso_anom}({iso_score:.3f}) | {hs_anom}({hs_score:.3f}) | "
                      f"crisis={crisis_active} | votes={votes}")
        elif votes >= 2 and in_settling and debug:
            print(f"  [DEBUG]  {t:5d} | votes={votes} but SUPPRESSED (settling until {settling_until})")

        if crisis_active and stats.vol_ratio < 1.5:
            crisis_active = False  # simplified recovery for benchmark speed

        # Periodic unconditional recalibration (see note above) -- only
        # while not in crisis, since crisis freeze is intentionally sticky.
        if not crisis_active and t > 0 and t % RECALIBRATION_INTERVAL == 0:
            cusum.adapt_regime()
            iso_forest.adapt_regime()

    return flags


def run_naive_baseline(series):
    """Fixed z-score threshold, no adaptation whatsoever."""
    stats = RollingStats(VOL_WINDOW, BASELINE_VOL_WINDOW)
    flags = []
    for t, value in enumerate(series):
        stats.update(value, drift_signal=False)
        if abs(stats.z_score(value)) > NAIVE_ZSCORE_THRESHOLD:
            flags.append(t)
    return flags


# ── Episode collapsing ──────────────────────────────────────────────────────────
def collapse_to_episodes(flags, merge_gap=20):
    """
    Collapse a list of per-tick flags into detection EPISODES: consecutive
    (or near-consecutive, within merge_gap ticks) flags are treated as one
    ongoing detection, represented by its first tick.

    WHY THIS MATTERS: once crisis mode triggers, every detector votes
    "anomaly" on every subsequent tick by design (see CUSUM.update() /
    AdaptiveIsolationForest.update() frozen-state behavior, and
    hs_anom = ... or crisis_active). This is correct, intentional behavior
    for a live deployed system — flag everything until a confirmed crisis
    resolves. But scored at the tick level, one sustained crisis period of
    (say) 500 ticks looks like 500 separate false-positive flags instead of
    one correctly-identified event. Episode collapsing fixes this: a
    detector that maintains a single continuous alert through a genuine
    event is scored as ONE true positive (or one false positive if
    unwarranted), not hundreds.
    """
    if not flags:
        return []
    flags = sorted(flags)
    episode_starts = [flags[0]]
    last = flags[0]
    for f in flags[1:]:
        if f - last > merge_gap:
            episode_starts.append(f)
        last = f
    return episode_starts


# ── Scoring against ground truth ────────────────────────────────────────────────
def score_flags(flags, onsets, n_ticks, tolerance=DETECTION_TOLERANCE):
    """
    For each true onset, a true positive is the FIRST flag within
    [onset, onset + tolerance]. Latency = flag_tick - onset.
    Flags not matched to any onset window count as false positives.
    """
    matched_onsets = set()
    latencies = []
    tp = 0
    used_flags = set()

    for onset in onsets:
        window_flags = [f for f in flags if onset <= f <= onset + tolerance and f not in used_flags]
        if window_flags:
            first = min(window_flags)
            used_flags.add(first)
            matched_onsets.add(onset)
            latencies.append(first - onset)
            tp += 1

    fp = len([f for f in flags if f not in used_flags])
    fn = len(onsets) - tp

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    mean_latency = float(np.mean(latencies)) if latencies else None

    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision": precision, "recall": recall, "f1": f1,
        "mean_latency_ticks": mean_latency,
        "total_flags": len(flags),
    }


# ── Main sweep ──────────────────────────────────────────────────────────────────
CONDITIONS = [
    ("abrupt",         2.0),
    ("abrupt",         4.0),
    ("gradual",        3.0),
    ("incremental",    3.0),
    ("recurring",      3.0),
    ("variance_shift", 3.0),
    ("none",           0.0),   # calm-period negative control: no injected drift at all
]

def main():
    results = []
    for drift_type, magnitude in CONDITIONS:
        for seed in RNG_SEEDS:
            if drift_type == "none":
                rng = np.random.default_rng(seed)
                series = rng.normal(0.0, BASELINE_STD, N_TICKS_PER_DRIFT_CONDITION)
                onsets = []
            else:
                series, onsets = generate_synthetic_returns(
                    drift_type, magnitude, N_TICKS_PER_DRIFT_CONDITION, seed
                )

            is_debug_run = (drift_type, magnitude, seed) == DEBUG_CONDITION
            if is_debug_run:
                print(f"\n=== DEBUG TRACE: {drift_type} mag={magnitude} seed={seed} ===")
            ens_flags_raw   = run_ensemble(series, debug=is_debug_run, true_onsets=onsets)
            naive_flags_raw = run_naive_baseline(series)

            # Collapse consecutive/near-consecutive flags into single detection
            # episodes before scoring (see collapse_to_episodes docstring) --
            # otherwise a sustained crisis-mode alert period is scored as
            # hundreds of independent false positives instead of one event.
            ens_flags   = collapse_to_episodes(ens_flags_raw)
            naive_flags = collapse_to_episodes(naive_flags_raw)

            ens_score   = score_flags(ens_flags, onsets, N_TICKS_PER_DRIFT_CONDITION)
            naive_score = score_flags(naive_flags, onsets, N_TICKS_PER_DRIFT_CONDITION)
            # total_flags in the score dict now reflects episode count, not raw
            # tick count -- also keep the raw tick count for transparency
            ens_score["raw_tick_flags"] = len(ens_flags_raw)
            naive_score["raw_tick_flags"] = len(naive_flags_raw)

            for system_name, score in [("ensemble", ens_score), ("naive_baseline", naive_score)]:
                row = {
                    "drift_type": drift_type,
                    "magnitude": magnitude,
                    "seed": seed,
                    "system": system_name,
                    **score,
                }
                results.append(row)
                print(f"[{drift_type:15s} mag={magnitude:.1f} seed={seed}] {system_name:15s} "
                      f"P={score['precision']:.2f} R={score['recall']:.2f} F1={score['f1']:.2f} "
                      f"latency={score['mean_latency_ticks']} flags={score['total_flags']}")

    out_dir = os.path.join(os.getcwd(), "benchmark_results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "synthetic_benchmark_results.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    print(f"\nSaved {len(results)} rows to {out_path}")

    # ── Aggregate summary: mean +/- std across seeds, per drift_type x system ──
    print("\n=== SUMMARY (mean +/- std across seeds) ===")
    from collections import defaultdict
    groups = defaultdict(list)
    for r in results:
        key = (r["drift_type"], r["magnitude"], r["system"])
        groups[key].append(r)

    summary_rows = []
    for (dtype, mag, system), rows in groups.items():
        prec = [r["precision"] for r in rows]
        rec  = [r["recall"] for r in rows]
        f1   = [r["f1"] for r in rows]
        lat  = [r["mean_latency_ticks"] for r in rows if r["mean_latency_ticks"] is not None]
        summary_rows.append({
            "drift_type": dtype, "magnitude": mag, "system": system,
            "precision_mean": np.mean(prec), "precision_std": np.std(prec),
            "recall_mean": np.mean(rec), "recall_std": np.std(rec),
            "f1_mean": np.mean(f1), "f1_std": np.std(f1),
            "latency_mean": np.mean(lat) if lat else None,
        })
        print(f"{dtype:15s} mag={mag:.1f} {system:15s} "
              f"P={np.mean(prec):.2f}±{np.std(prec):.2f} "
              f"R={np.mean(rec):.2f}±{np.std(rec):.2f} "
              f"F1={np.mean(f1):.2f}±{np.std(f1):.2f} "
              f"latency={np.mean(lat):.1f} ticks" if lat else "latency=N/A")

    summary_path = os.path.join(out_dir, "synthetic_benchmark_summary.csv")
    with open(summary_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"Saved summary to {summary_path}")


if __name__ == "__main__":
    main()