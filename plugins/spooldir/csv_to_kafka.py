import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime

# Load your CSV file
df = pd.read_csv("C:/Users/RiteshKumar/AveHealth/data/data.csv")

# Add a timestamp field
df['processing_timestamp'] = datetime.utcnow().isoformat()

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send each row as a flat JSON object
for _, row in df.iterrows():
    message = row.to_dict()
    print("Sending:", message)
    producer.send('csv-topic', value=message)

producer.flush()
print("All messages sent successfully.")