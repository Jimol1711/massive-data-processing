from kafka import KafkaConsumer
from collections import defaultdict, deque
import json, time, re, sys

from datetime import datetime

WINDOW_SIZE = 60  # segundos
STEP = 1          # imprimir cada 1s
topic = sys.argv[1]  # TÃ³pico pasado como argumento

# hashtag -> deque[timestamps]
hashtag_events = defaultdict(deque)

def extract_hashtags(text):
    return re.findall(r'#\w+', text.lower())

def clean_old(current_time):
    for tag in list(hashtag_events):
        while hashtag_events[tag] and current_time - hashtag_events[tag][0] > WINDOW_SIZE:
            hashtag_events[tag].popleft()
        if not hashtag_events[tag]:
            del hashtag_events[tag]

def trending_top10(current_time):
    counts = []
    for tag, timestamps in hashtag_events.items():
        count = sum(1 for ts in timestamps if current_time - ts <= WINDOW_SIZE)
        if count > 0:
            counts.append((tag, count))
    
    counts.sort(key=lambda x: (-x[1], x[0]))

    top10 = [(tag, count / WINDOW_SIZE) for tag, count in counts[:10]]
    return top10

# Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers='c1:9092',  # <-- tu entorno
    value_deserializer=lambda v: v.decode('utf-8'),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='top10-trending'
)

last_print = time.time()

for msg in consumer:
    line = msg.value
    parts = line.split('\t')

    if len(parts) >= 7:
        try:
            dt = datetime.strptime(parts[1], "%Y-%m-%d %H:%M:%S")
            ts = int(dt.timestamp())
            text = parts[6]  # columna 7: tweet text
            hashtags = extract_hashtags(text)

            for tag in hashtags:
                hashtag_events[tag].append(ts)

            now = time.time()
            if now - last_print >= STEP:
                print(".")
                clean_old(ts)
                top = trending_top10(ts)
                print(f"[{datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}] Top Trending Hashtags:")
                for i, (tag, ratio) in enumerate(top, 1):
                    print(f"{i}. {tag} (↑ +{ratio:.1f}x)")
                print("-" * 40)
                last_print = now

        except Exception as e:
            print(f"Error processing tweet: {e}")
