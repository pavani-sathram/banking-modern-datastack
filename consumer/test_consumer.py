from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'banking_server.public.customers',
    bootstrap_servers=['172.18.0.6:29092'],
    auto_offset_reset='earliest',
    group_id='test-python-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000,
    max_poll_records=10
)

print("Starting consumer...")
count = 0
for message in consumer:
    if message is None:
        print(f"\nTimeout after {count} messages")
        break
    count += 1
    print(f"Message {count}: {message.topic} partition {message.partition} offset {message.offset}")
    if count >= 5:
        break

print(f"Total: {count} messages")
consumer.close()
