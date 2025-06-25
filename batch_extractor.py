"""
batch_extractor.py

This script consumes messages from Kafka topics (`pin_data.pin`, `pin_data.geo`, `pin_data.user`)
and matches them by their shared 'idx' key. It collects 500 fully aligned triplets and writes
them to CSV files for downstream analysis.

This simulates batch data extraction in a production-style pipeline.

Author: Shahid Hadi
"""

import pandas as pd
from kafka import KafkaConsumer
from json import loads
from collections import defaultdict


def extract_500_messages_per_topic():
    """
    Extracts 500 complete rows of aligned 'pin', 'geo', and 'user' data from Kafka topics.
    Matches records by the shared `idx` key and writes output to CSV files.

    Returns:
        tuple: DataFrames for pin, geo, and user data respectively.
    """
    topics = ['pin_data.pin', 'pin_data.geo', 'pin_data.user']
    consumers = {}
    data_by_idx = defaultdict(dict)
    complete_rows = set()

    for topic in topics:
        consumers[topic] = KafkaConsumer(
            topic,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"{topic}_collector_group",
            value_deserializer=lambda m: loads(m.decode('utf-8'))
        )

    print("Listening for messages...")

    while len(complete_rows) < 500:
        for topic in topics:
            msg_pack = consumers[topic].poll(timeout_ms=500, max_records=10)
            for _, messages in msg_pack.items():
                for msg in messages:
                    data = msg.value
                    idx = data.get("idx")
                    if not idx:
                        continue
                    topic_key = topic.split('.')[-1]
                    data_by_idx[idx][topic_key] = data
                    if all(k in data_by_idx[idx] for k in ['pin', 'geo', 'user']):
                        complete_rows.add(idx)
        print(f"{len(complete_rows)} complete rows collected...")

    for consumer in consumers.values():
        consumer.close()

    pins, geos, users = [], [], []
    for idx in list(complete_rows)[:500]:
        pins.append(data_by_idx[idx]['pin'])
        geos.append(data_by_idx[idx]['geo'])
        users.append(data_by_idx[idx]['user'])

    df_pin = pd.DataFrame(pins)
    df_geo = pd.DataFrame(geos)
    df_user = pd.DataFrame(users)

    df_pin.to_csv("batch_data/pin_data.csv", index=False)
    df_geo.to_csv("batch_data/geo_data.csv", index=False)
    df_user.to_csv("batch_data/user_data.csv", index=False)

    print("Aligned extraction complete.")
    return df_pin, df_geo, df_user


if __name__ == "__main__":
    """
    Main entry point for batch extraction. Extracts 500 full rows and prints summary info.
    """
    df_pin, df_geo, df_user = extract_500_messages_per_topic()
    print(df_pin.info())
    print(df_geo.info())
    print(df_user.info())
