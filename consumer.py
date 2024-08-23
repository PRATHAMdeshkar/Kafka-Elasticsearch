from kafka import KafkaConsumer
import json
from config.kafka_config import KAFKA_BROKER_URL, KAFKA_TOPIC

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER_URL,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='log-consumer-group'
)

def consume_logs():
    print("consumer: consuming...")
    for message in consumer:
        log = message.value
        timestamp = log.get('timestamp', 'No timestamp found')
        print(timestamp)
if __name__ == '__main__':
    consume_logs()
