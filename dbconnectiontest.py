import os
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST"),
    port=os.getenv("TIMESCALE_PORT"),
    user=os.getenv("TIMESCALE_USER"),
    password=os.getenv("TIMESCALE_PASSWORD"),
    dbname=os.getenv("TIMESCALE_DB"),
)
cur = conn.cursor()

consumer = KafkaConsumer(
    "binance-btcusdt",
    bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAP"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("CONFLUENT_API_KEY"),
    sasl_plain_password=os.getenv("CONFLUENT_API_SECRET"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
)

print("Consumer started — listening on mcx-crude-prices")

for msg in consumer:
    tick = msg.value
    cur.execute(
        "INSERT INTO crude_ticks (time, symbol, price, volume) VALUES (%s, %s, %s, %s)",
        (tick["timestamp"], tick["symbol"], tick["price"], tick["volume"]),
    )
    conn.commit()
    print(f"Inserted: {tick}")