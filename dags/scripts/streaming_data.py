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

# Generate Fraud Cases
def generate_fraud_case():
    return {
        "fraud_id": fake.uuid4(),
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "fraud_reason": fake.sentence(),
        "fraud_detected_by": random.choice(["rule_engine", "machine_learning", "manual_review"]),
        "detected_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "action_taken": random.choice(["account_freeze", "refund", "report_to_authorities"]),
    }
    

# Generate Device Info
def generate_device_info():
    return {
        "device_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "is_rooted": random.choice([True, False]),
        "last_used_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
    

# Generate IP Activity
def generate_ip_activity():
    return {
        "ip_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "ip_address": fake.ipv4(),
        "login_time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "failed_attempts": random.randint(0, 5),
        "location": fake.city(),
        "vpn_used": random.choice([True, False]),
    }

for _ in range(100):
    producer.send("transactions", generate_transaction())
    producer.send("fraud_cases", generate_fraud_case())
    producer.send("device_info",generate_device_info())
    producer.send("ip_activity",generate_ip_activity())

producer.flush()
print("Streaming started to Kafka.")
