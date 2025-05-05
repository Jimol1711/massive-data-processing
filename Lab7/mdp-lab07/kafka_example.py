from kafka import KafkaProducer, KafkaConsumer
import threading
import time
import sys
import datetime

topic = sys.argv[1]  # Tópico pasado como argumento

def produce():
    producer = KafkaProducer(
        bootstrap_servers='c1:9092',
        key_serializer=lambda k: str(k).encode('utf-8'),
        value_serializer=lambda v: str(v).encode('utf-8')
    )
    
    i = 0
    while True:
        d = datetime.datetime.now()
        value = d.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        producer.send(topic, key=i, value=value)
        print(f"Message produced: key={i}, value={value}")
        i += 1
        time.sleep(1)
    # Opcional: espera a que se envíen todos los mensajes
    # producer.flush()

def consume():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='c1:9092',
        auto_offset_reset='earliest',
        group_id='default_consumer_group',
        enable_auto_commit=True,
        key_deserializer=lambda k: int(k.decode('utf-8')) if k else None,
        value_deserializer=lambda v: v.decode('utf-8')
    )

    for msg in consumer:
        print(f"{msg.topic} [{msg.partition}] offset={msg.offset}, key={msg.key}, value=\"{msg.value}\"\n")

# Ejecutar productor en hilo separado
producer_thread = threading.Thread(target=produce)
producer_thread.start()

# Consumidor en hilo principal
consume()
