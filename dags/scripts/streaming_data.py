from faker import Faker
import json
import random
from datetime import datetime
from kafka import KafkaProducer

fake = Faker()

KAFKA_BROKER = "localhost:9092"
TOPICS = ["transactions", "fraud_cases", "device_info", "ip_activity"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "amount": round(random.uniform(10, 5000), 2),
        "currency": "USD",
        "transaction_type": random.choice(["purchase", "withdrawal", "deposit", "transfer"]),
        "transaction_time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "payment_method": random.choice(["credit_card", "debit_card"]),
        "status": "completed",
        "is_fraudulent": random.choice([True, False]),
    }

for _ in range(100):
    producer.send("transactions", generate_transaction())

producer.flush()
print("Streaming started to Kafka.")
