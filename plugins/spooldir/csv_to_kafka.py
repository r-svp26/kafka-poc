import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime

# Load the CSV file
df = pd.read_csv('C:/Users/RiteshKumar/AveHealth/data/data.csv')


# Add processing timestamp
df['processing_timestamp'] = datetime.utcnow().isoformat()

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send each row as JSON to the Kafka topic
for _, row in df.iterrows():
    producer.send('csv-topic', value=row.to_dict())
    print(f"Sent: {row.to_dict()}")

producer.flush()
producer.close()
