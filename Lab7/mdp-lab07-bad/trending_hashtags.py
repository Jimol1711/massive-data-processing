from kafka import KafkaConsumer
from collections import defaultdict, deque
import json, time, re, sys

from datetime import datetime

WINDOW_SIZE = 60  # segundos
STEP = 1          # imprimir cada 1s
topic = sys.argv[1]  # Topico pasado como argumento

# hashtag -> deque[timestamps]
hashtag_events = defaultdict(deque)

def extract_hashtags(text):
    return re.findall(r'#\w+', text.lower())

def clean_old(current_time):
    for tag in list(hashtag_events):
        while hashtag_events[tag] and current_time - hashtag_events[tag][0] > 2 * WINDOW_SIZE:
            hashtag_events[tag].popleft()
        if not hashtag_events[tag]:
            del hashtag_events[tag]

def trending_top10(current_time):
    trending = []

    for tag, timestamps in hashtag_events.items():
        """
        count_recent = sum(1 for ts in timestamps if current_time - ts <= WINDOW_SIZE)
        count_previous = sum(1 for ts in timestamps if WINDOW_SIZE < current_time - ts <= 2 * WINDOW_SIZE)

        if count_previous == 0:
            ratio = float('inf') if count_recent > 0 else 0.0
        else:
            ratio = count_recent / count_previous

        if count_recent > 0:
            trending.append((tag, ratio))
        """
        count = sum(1 for ts in timestamps if current_time - ts <= WINDOW_SIZE)
        if count > 0:
            trending.append((tag, count))

    trending.sort(key=lambda x: (-x[1], x[0]))

    return trending[:10]

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
    parts = line.split() # line.split('\t')

    # debug line print
    # print("RAW TWEET:", line)

    if len(parts) >= 8:
        try:
            timestamp_str = parts[0] + ' ' + parts[1]
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S") # datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
            ts = int(dt.timestamp())
            text = ' '.join(parts[6:]) # parts[6]  # columna 7: tweet text
            hashtags = extract_hashtags(text)

            for tag in hashtags:
                hashtag_events[tag].append(ts)

            now = ts # time.time()
            if now - last_print >= STEP:
                clean_old(now)
                top = trending_top10(now)
                print("[", datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S'),"] Top Trending Hashtags:")
                for i, (tag, ratio) in enumerate(top, 1):
                    # print(f"{i}. {tag} (<flecha arriba> +{ratio:.1f}x)")
                    print(i, ".", tag, "(<flecha arriba> +", round(ratio, 1), "x)")
                print("-" * 40)
                last_print = now

        except Exception as e:
            print("Error processing tweet:", e)
