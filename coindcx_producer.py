import websocket
import json
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAP"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("CONFLUENT_API_KEY"),
    sasl_plain_password=os.getenv("CONFLUENT_API_SECRET"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# TOPIC = "mcx-crude-prices"
TOPIC = "binance-btcusdt"

def on_message(ws, message):
    data = json.loads(message)
    tick = {
        "symbol": data["s"],
        "price": float(data["c"]),
        "volume": float(data["v"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    producer.send(TOPIC, value=tick)
    producer.flush()
    print(f"Live: {tick['symbol']} @ {tick['price']}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, code, msg):
    print("Closed")

def on_open(ws):
    print("Connected to Binance WebSocket")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws/btcusdt@ticker",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()