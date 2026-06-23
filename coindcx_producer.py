import os
import json
import time
import math
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
print("***** PRODUCER FILE EXECUTING *****")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAP"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("CONFLUENT_API_KEY"),
    sasl_plain_password=os.getenv("CONFLUENT_API_SECRET"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = "mcx-crude-prices"
print(f"Starting producer. Topic = {TOPIC}")

t = 0

while True:
    t += 1

    # Normal regime — oscillating price with noise
    price = 6500 + 50 * math.sin(t / 10) + random.gauss(0, 20)

    # Sudden drift at tick 100 — sharp jump
    if t == 100:
        price += 500
        print("*** SUDDEN DRIFT INJECTED at tick 100 ***")

    # Gradual drift after tick 200 — slow upward creep
    if t > 200:
        price += (t - 200) * 0.5

    # Recurring drift — periodic shock every 300 ticks
    if t % 300 == 0:
        price += random.choice([-300, 300])
        print(f"*** RECURRING DRIFT INJECTED at tick {t} ***")

    tick = {
        "symbol": "CRUDEOIL",
        "price": round(price, 2),
        "volume": round(random.uniform(80, 120), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    producer.send(TOPIC, value=tick)
    producer.flush()
    print(f"Tick {t}: Sent price {tick['price']}")

    time.sleep(1)