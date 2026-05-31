#!/usr/bin/env python3
import subprocess
import json
import boto3
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")

print(f"🔧 Configuration:")
print(f"  MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"  MinIO Bucket: {MINIO_BUCKET}")
print()

# MinIO client
try:
    print("🔗 Connecting to MinIO...")
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )
    
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if MINIO_BUCKET not in buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"✅ Created MinIO bucket: {MINIO_BUCKET}")
    else:
        print(f"✅ Connected to MinIO! Bucket '{MINIO_BUCKET}' exists.")
except Exception as e:
    print(f"❌ Failed to connect to MinIO: {e}")
    exit(1)

print()

# Use docker exec to consume from inside the network
print("📡 Consuming from Kafka via Docker...\n")

topics = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions'
]

for topic in topics:
    print(f"Processing {topic}...")
    
    try:
        # Use docker exec to run kafka-console-consumer inside the container network
        result = subprocess.run(
            [
                '/Applications/Docker.app/Contents/Resources/bin/docker', 'exec', 'kafka', 'kafka-console-consumer',
                '--bootstrap-server', 'localhost:9092',
                '--topic', topic,
                '--from-beginning',
                '--timeout-ms', '10000',
                '--max-messages', '1000'
            ],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0 or 'Processed' in result.stderr:
            messages = result.stdout.strip().split('\n')
            records = []
            
            for msg in messages:
                if msg.strip():
                    try:
                        data = json.loads(msg)
                        if 'payload' in data and 'after' in data['payload']:
                            record = data['payload']['after']
                            if record:
                                records.append(record)
                    except:
                        pass
            
            if records:
                # Write to MinIO
                import pandas as pd
                df = pd.DataFrame(records)
                date_str = datetime.now().strftime('%Y-%m-%d')
                file_path = f'{topic.split(".")[-1]}_{date_str}.parquet'
                
                df.to_parquet(file_path, engine='pyarrow', index=False)
                
                table_name = topic.split('.')[-1]
                s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
                
                s3.upload_file(file_path, MINIO_BUCKET, s3_key)
                os.remove(file_path)
                
                print(f'✅ Uploaded {len(records)} records from {topic} to s3://{MINIO_BUCKET}/{s3_key}')
            else:
                print(f"⚠️  No valid records found in {topic}")
    except subprocess.TimeoutExpired:
        print(f"⏱️  Timeout reading {topic}")
    except Exception as e:
        print(f"❌ Error processing {topic}: {e}")

print("\n✅ Done!")
