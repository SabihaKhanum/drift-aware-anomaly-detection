import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Drift-Aware Anomaly Detection", layout="wide")
st.title("Drift-Aware Anomaly Detection — Live Monitor")

conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"), port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"), password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB")
)

# Auto-refresh every 5 seconds
st.button("Refresh")

col1, col2, col3 = st.columns(3)

# ── Stats ──────────────────────────────────────────────
total_anomalies = pd.read_sql(
    "SELECT COUNT(*) as n FROM anomaly_alerts WHERE detector='Ensemble' AND is_anomaly=true",
    conn).iloc[0]['n']
total_drift = pd.read_sql(
    "SELECT COUNT(*) as n FROM drift_events", conn).iloc[0]['n']
avg_latency = pd.read_sql(
    "SELECT COALESCE(ROUND(AVG(adaptation_latency_s)::numeric, 3), 0) as l "
    "FROM drift_events WHERE adaptation_latency_s IS NOT NULL",
    conn).iloc[0]['l']

col1.metric("Ensemble Anomalies", total_anomalies)
col2.metric("Drift Events", total_drift)
col3.metric("Avg Adaptation Latency", f"{avg_latency}s")

# ── Price + anomalies ──────────────────────────────────
st.subheader("Live Price + Anomaly Alerts")
prices = pd.read_sql("""
    SELECT time, price, is_anomaly, severity_context
    FROM anomaly_alerts WHERE detector='Ensemble'
    ORDER BY time DESC LIMIT 500
""", conn)
prices = prices.sort_values("time")

fig = px.line(prices, x="time", y="price", title="BTC/USDT Price")
anomalies = prices[prices["is_anomaly"]]
fig.add_scatter(x=anomalies["time"], y=anomalies["price"],
                mode="markers", marker=dict(color="red", size=8),
                name="Anomaly")
st.plotly_chart(fig, use_container_width=True)

# ── Scores per detector ────────────────────────────────
st.subheader("Anomaly Scores by Detector")
scores = pd.read_sql("""
    SELECT time, detector, score FROM anomaly_alerts
    WHERE detector != 'Ensemble'
    ORDER BY time DESC LIMIT 1000
""", conn)
scores = scores.sort_values("time")
fig2 = px.line(scores, x="time", y="score", color="detector")
st.plotly_chart(fig2, use_container_width=True)

# ── Drift events ───────────────────────────────────────
st.subheader("Drift Events")
col4, col5 = st.columns(2)

drift_df = pd.read_sql("""
    SELECT time, severity, adaptation_strategy, 
           vol_ratio, z_score, adaptation_latency_s
    FROM drift_events ORDER BY time DESC LIMIT 50
""", conn)
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

# ── Adaptation latency over time ───────────────────────
st.subheader("Adaptation Latency per Drift Event")
lat_df = pd.read_sql("""
    SELECT time, adaptation_latency_s, severity
    FROM drift_events WHERE adaptation_latency_s IS NOT NULL
    ORDER BY time
""", conn)
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

conn.close()