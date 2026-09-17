import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Drift-Aware Anomaly Detection", layout="wide")
st.title("Drift-Aware Anomaly Detection — Live Monitor")

conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"), port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"), password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB")
)

# ── View mode: Live vs Backtest ─────────────────────────
# Live and backtest rows sit in the SAME tables, distinguished only by
# `time`. Without a filter, "ORDER BY time DESC LIMIT N" always surfaces
# the live (most recent) rows -- a 2022 backtest run becomes invisible
# the moment any live 2026 data exists. This toggle makes the query
# window explicit instead of implicit.
st.sidebar.header("View settings")
view_mode = st.sidebar.radio("Data view", ["Live (latest)", "Backtest date range"])

if view_mode == "Backtest date range":
    default_start = date(2022, 5, 19)
    default_end = date(2022, 5, 19) + timedelta(days=1)
    start_date = st.sidebar.date_input("Start date", default_start)
    end_date = st.sidebar.date_input("End date", default_end)
    time_filter_sql = "WHERE time >= %s AND time < %s"
    time_params = (start_date, end_date)
    row_limit = 100000  # backtest windows are bounded by date, not row count
else:
    time_filter_sql = ""
    time_params = ()
    row_limit = st.sidebar.slider("Rows to show (live)", 100, 2000, 500, step=100)

# Optional symbol filter, useful once multiple symbols share the tables
symbols = pd.read_sql("SELECT DISTINCT symbol FROM anomaly_alerts ORDER BY symbol", conn)
symbol_options = ["All"] + symbols["symbol"].tolist()
selected_symbol = st.sidebar.selectbox("Symbol", symbol_options)

def build_where(base_alias_has_time=True, extra=""):
    """Combine the time-range filter with an optional extra clause/symbol filter."""
    clauses = []
    params = []
    if time_filter_sql:
        clauses.append("time >= %s AND time < %s")
        params.extend(time_params)
    if selected_symbol != "All":
        clauses.append("symbol = %s")
        params.append(selected_symbol)
    if extra:
        clauses.append(extra)
    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params

st.button("Refresh")

col1, col2, col3 = st.columns(3)

# ── Stats ──────────────────────────────────────────────
where_sql, params = build_where(extra="detector='Ensemble' AND is_anomaly=true")
total_anomalies = pd.read_sql(
    f"SELECT COUNT(*) as n FROM anomaly_alerts {where_sql}",
    conn, params=params).iloc[0]['n']

# drift_events has no `symbol` column, so build its own time-only filter
drift_where = "WHERE time >= %s AND time < %s" if time_filter_sql else ""
drift_params = list(time_params)

total_drift = pd.read_sql(
    f"SELECT COUNT(*) as n FROM drift_events {drift_where}",
    conn, params=drift_params).iloc[0]['n']

avg_latency_where = drift_where + (" AND " if drift_where else "WHERE ") + "adaptation_latency_s IS NOT NULL"
avg_latency = pd.read_sql(
    f"SELECT COALESCE(ROUND(AVG(adaptation_latency_s)::numeric, 3), 0) as l "
    f"FROM drift_events {avg_latency_where}",
    conn, params=drift_params).iloc[0]['l']

col1.metric("Ensemble Anomalies", total_anomalies)
col2.metric("Drift Events", total_drift)
col3.metric("Avg Adaptation Latency", f"{avg_latency}s")

# ── Price + anomalies ──────────────────────────────────
st.subheader("Live Price + Anomaly Alerts" if view_mode == "Live (latest)" else "Backtest Price + Anomaly Alerts")
where_sql, params = build_where(extra="detector='Ensemble'")
order_dir = "DESC" if view_mode == "Live (latest)" else "ASC"
prices = pd.read_sql(f"""
    SELECT time, price, is_anomaly, severity_context
    FROM anomaly_alerts {where_sql}
    ORDER BY time {order_dir} LIMIT %s
""", conn, params=params + [row_limit])
prices = prices.sort_values("time")

# fig = px.line(prices, x="time", y="price", title="BTC/USDT Price")
# anomalies = prices[prices["is_anomaly"]]
# fig.add_scatter(x=anomalies["time"], y=anomalies["price"],
#                 mode="markers", marker=dict(color="red", size=8),
#                 name="Anomaly")
# st.plotly_chart(fig, use_container_width=True)
prices = prices.sort_values("time")

fig = px.line(prices, x="time", y="price", title="BTC/USDT Price")
anomalies = prices[prices["is_anomaly"]]
if not anomalies.empty:
    fig.add_scatter(x=anomalies["time"], y=anomalies["price"],
                    mode="markers", marker=dict(color="red", size=8),
                    name="Anomaly")
st.plotly_chart(fig, use_container_width=True)
# ── Scores per detector ────────────────────────────────
st.subheader("Anomaly Scores by Detector")
where_sql, params = build_where(extra="detector != 'Ensemble'")
scores = pd.read_sql(f"""
    SELECT time, detector, score FROM anomaly_alerts
    {where_sql}
    ORDER BY time {order_dir} LIMIT %s
""", conn, params=params + [row_limit * 3])  # 3 non-ensemble detectors per tick
scores = scores.sort_values("time")
fig2 = px.line(scores, x="time", y="score", color="detector")
st.plotly_chart(fig2, use_container_width=True)

# ── Per-detector flag rate summary (useful for threshold tuning) ──
st.subheader("Flag Rate by Detector")
where_sql, params = build_where()
flag_rates = pd.read_sql(f"""
    SELECT detector,
           COUNT(*) FILTER (WHERE is_anomaly) AS flags,
           COUNT(*) AS total,
           ROUND(100.0 * COUNT(*) FILTER (WHERE is_anomaly) / COUNT(*), 2) AS pct
    FROM anomaly_alerts
    {where_sql}
    GROUP BY detector
    ORDER BY detector
""", conn, params=params)
st.dataframe(flag_rates, use_container_width=True)

# ── Drift events ───────────────────────────────────────
st.subheader("Drift Events")
col4, col5 = st.columns(2)

drift_order = "DESC" if view_mode == "Live (latest)" else "ASC"
drift_limit = 50 if view_mode == "Live (latest)" else 5000
drift_df = pd.read_sql(f"""
    SELECT time, severity, adaptation_strategy,
           vol_ratio, z_score, adaptation_latency_s
    FROM drift_events {drift_where}
    ORDER BY time {drift_order} LIMIT %s
""", conn, params=drift_params + [drift_limit])
col4.dataframe(drift_df)

if not drift_df.empty and "severity" in drift_df.columns:
    sev_counts = drift_df["severity"].value_counts().reset_index()
    sev_counts.columns = ["severity", "count"]
    fig3 = px.pie(sev_counts, names="severity", values="count",
                  title="Drift Severity Distribution",
                  color="severity",
                  color_discrete_map={
                      "INTRADAY_NOISE": "#10B981",
                      "REGIME_SHIFT":   "#F59E0B",
                      "CRISIS":         "#EF4444"
                  })
    col5.plotly_chart(fig3)
else:
    col5.info("No drift events in this window.")

# ── Adaptation latency over time ───────────────────────
st.subheader("Adaptation Latency per Drift Event")
lat_where = drift_where + (" AND " if drift_where else "WHERE ") + "adaptation_latency_s IS NOT NULL"
lat_df = pd.read_sql(f"""
    SELECT time, adaptation_latency_s, severity
    FROM drift_events {lat_where}
    ORDER BY time
""", conn, params=drift_params)
if not lat_df.empty:
    fig4 = px.scatter(lat_df, x="time", y="adaptation_latency_s",
                      color="severity",
                      color_discrete_map={
                          "INTRADAY_NOISE": "#10B981",
                          "REGIME_SHIFT":   "#F59E0B",
                          "CRISIS":         "#EF4444"
                      },
                      title="Adaptation Latency (seconds)")
    st.plotly_chart(fig4, use_container_width=True)
else:
    st.info("No adaptation-latency data in this window.")

conn.close()