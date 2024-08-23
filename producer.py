import pytz
from kafka import KafkaProducer
import json
from datetime import datetime
from config.kafka_config import KAFKA_BROKER_URL, KAFKA_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
utc_time = datetime.utcnow().replace(tzinfo=pytz.utc)
ist = pytz.timezone('Asia/Kolkata')
ist_time = utc_time.astimezone(ist)
formatted_timestamp = ist_time.strftime('%d/%m/%Y %I:%M:%S %p')


def send_log():
    log = {
        'timestamp': formatted_timestamp
    }
    producer.send(KAFKA_TOPIC, value=log)
    producer.flush()

if __name__ == '__main__':
    send_log()
