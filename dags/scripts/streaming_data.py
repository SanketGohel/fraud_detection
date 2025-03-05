from faker import Faker
import json
import random
from datetime import datetime
import boto3
fake = Faker()

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
        "d": fake.uuid4(),
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

kinesis_client = boto3.client("kinesis", region_name = "us-east-1")

TOPICS = {"transaction":generate_transaction(), "fraudCases":generate_fraud_case(),"de": generate_device_info(),"ipActvity": generate_ip_activity()}

print("Streaming started to Kinesis.")

for _ in range(1000):
    for table,data in TOPICS.items():
        stream_name = f"fraud-project-{table}-stream"
        response = kinesis_client.put_record(
            StreamName = stream_name,
            Data = json.dumps(data),
            PartitionKey = data.get("transaction_time") if table == 'transaction' else "defaut_key")
    print(f"Sent Record: {table}")
