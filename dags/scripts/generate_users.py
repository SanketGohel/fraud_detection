import pandas as pd
import boto3
import random
from faker import Faker
import os
from datetime import datetime


import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Initialize Faker
fake = Faker()

# AWS S3 Configuration

S3_BUCKET = config['DEV']['S3_BUCKET'] 
S3_FILE_PATH = "delta/users/users.parquet"

# Function to generate user data
def generate_users(n=1000):
    users = []
    for _ in range(n):
        user = {
            "user_id": _ + 1,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),
            "country": fake.country(),
            "signup_date": fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            "account_status": random.choice(["active", "suspended", "closed"]),
        }
        users.append(user)

    return pd.DataFrame(users)

# Generate 1000 users
users_df = generate_users(1000)

# Save Data as Parquet
parquet_file = "/tmp/users.parquet"
users_df.to_parquet(parquet_file, index=False)

# Upload to S3
s3 = boto3.client("s3")
s3.upload_file(parquet_file, S3_BUCKET, S3_FILE_PATH)

print(f"User data successfully uploaded to s3://{S3_BUCKET}/{S3_FILE_PATH}")
