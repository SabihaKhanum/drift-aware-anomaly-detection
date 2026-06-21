import os
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
import websocket

load_dotenv()
print("***** PRODUCER FILE EXECUTING *****")
# Kafka producer setup — connects to Confluent Cloud
producer = KafkaProducer(
    bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAP"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("CONFLUENT_API_KEY"),
    sasl_plain_password=os.getenv("CONFLUENT_API_SECRET"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# TOPIC = "crypto.ticks.raw"
TOPIC = "mcx-crude-prices"
print(f"Starting producer. Topic = {TOPIC}")

def on_message(ws, message):
    data = json.loads(message)
    # CoinDCX sends ticker updates with market, price, timestamp
    tick = {
        "symbol": data.get("market", "BTCINR"),
        "price": float(data.get("last_price", 0)),
        "volume": float(data.get("volume", 0)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(TOPIC, value=tick)
    producer.flush()
    print(f"Sent: {tick}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connected — subscribing to BTCINR ticker")
    subscribe_msg = {
        "event": "subscribe",
        "data": {"channelName": "B-BTC_INR"}
    }
    ws.send(json.dumps(subscribe_msg))

# if __name__ == "__main__":
#     ws_url = "wss://stream.coindcx.com"
#     ws = websocket.WebSocketApp(
#         ws_url,
#         on_open=on_open,
#         on_message=on_message,
#         on_error=on_error,
#         on_close=on_close,
#     )
#     ws.run_forever()

import json
import time
from datetime import datetime, timezone

while True:
    tick = {
        "symbol": "CRUDEOIL",
        "price": 6500,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    producer.send(TOPIC, value=tick)
    producer.flush()

    print(f"Sent: {tick}")

    time.sleep(2)