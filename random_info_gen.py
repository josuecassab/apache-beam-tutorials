from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json import dumps
from random import randint
import time


fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['pkc-619z3.us-east1.gcp.confluent.cloud:9092'],
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='OMRL4EBMA2BQT762',
    sasl_plain_password='349+1RWttT1QVINg45ep4G6SYG/iCVnM+6lC+EzKRAXhGwClsgdOXqFmzxStZWyb',
)
records = []

for i in range(5):
    data = {
        'name': fake.name(),
        'email': fake.email(),
        'phone': fake.phone_number(),
        'address': fake.address(),
        'city': fake.city(),
        'timestamp': time.time(),
    }
    for i in range(randint(1, 4)):

        record = {**data,
                  'quantity': randint(1, 10),
                  'price': randint(100, 200),
                  }
        
        print(record)
        json_data = dumps(record).encode('utf-8')
        future = producer.send('topic_0', json_data)

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            print(e)
        
        # Successful result returns assigned partition and offset
        print (record_metadata.topic)
        print (record_metadata.partition)
        print (record_metadata.offset)