import yaml
import requests
import random
import sqlalchemy
import psycopg2
import pandas as pd
import uvicorn
from json import dumps, loads
from sqlalchemy import text
from kafka import KafkaConsumer
from time import sleep
from fastapi import FastAPI, Request
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from threading import Thread
from collections import defaultdict
from multiprocessing import Process

random.seed(100)


class AWSDBConnector:

    @staticmethod
    def read_db_creds():
        """
        Reads database credentials from the `db_creds.yaml` file.
        
        Returns:
            dict: A dictionary containing database credentials.
        """
        with open('db_creds.yaml', 'r') as db:
            db_creds_dict = yaml.load(db, Loader=yaml.SafeLoader)
        return db_creds_dict

    def __init__(self):

        db_creds_dict = AWSDBConnector.read_db_creds()

        # TODO  Remember to not upload these creds to github!
        #   Put them in a creds.yaml and read that file in instead.
        self.HOST = db_creds_dict['HOST']
        self.USER =  db_creds_dict['USER']
        self.PASSWORD =  db_creds_dict['PASSWORD']
        self.DATABASE =  db_creds_dict['DATABASE']
        self.PORT =  db_creds_dict ['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
        return engine


new_connector = AWSDBConnector()
app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest data producer",
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    'pin_data.geo',
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="geo_data_consumer",
    value_deserializer=lambda m: loads(m.decode('utf-8'))
)


def run_infinite_post_data_loop():
    while True:
        sleep(0.05)
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            try:
                # Get a single row from pinterest_data and extract the idx
                pin_result = dict(connection.execute(
                    text(f"SELECT * FROM pinterest_data LIMIT 1 OFFSET {random_row}")
                ).mappings().first())

                idx = pin_result['idx']

                # Fetch corresponding rows using the same idx
                geo_result = dict(connection.execute(
                    text("SELECT * FROM geolocation_data WHERE idx = :idx"),
                    {"idx": idx}
                ).mappings().first())

                user_result = dict(connection.execute(
                    text("SELECT * FROM user_data WHERE idx = :idx"),
                    {"idx": idx}
                ).mappings().first())

                if geo_result and user_result:
                    # Post geo data
                    requests.post(
                        "http://localhost:8000/send_data?topic=pin_data.geo",
                        data=dumps(geo_result, default=str),
                        headers={"Content-Type": "application/json"}
                    )

                    # Post user data
                    requests.post(
                        "http://localhost:8000/send_data?topic=pin_data.user",
                        data=dumps(user_result, default=str),
                        headers={"Content-Type": "application/json"}
                    )

                    # Post pin data
                    requests.post(
                        "http://localhost:8000/send_data?topic=pin_data.pin",
                        data=dumps(pin_result, default=str),
                        headers={"Content-Type": "application/json"}
                    )
                else:
                    print(f"[WARN] Missing geo/user data for idx: {idx}")

            except Exception as e:
                print(f"[ERROR] Failed to fetch or post data: {e}")

            

     
# ---------- FastAPI Routes ----------

@app.post("/send_data")
async def send_data(payload: dict, topic: str):
    try:
        producer.send(topic, value=payload)
        producer.flush()
        return {"status": "Message sent", "topic": topic}
    except Exception as e:
        return {"error": str(e)}

@app.get("/get_data")
def retrieve_data():
    msg = next(consumer)
    return {
        "topic": msg.topic,
        "data": msg.value
    }


# ---------- Start App and Threads ----------

def run_webserver():
    uvicorn.run(app, host="localhost", port=8000)


# --------- Batch Extraction ----------------

import pandas as pd
from kafka import KafkaConsumer
from json import loads

def extract_500_messages_per_topic():
    topics = ['pin_data.pin', 'pin_data.geo', 'pin_data.user']
    consumers = {}

    # Dictionary to hold messages grouped by idx
    data_by_idx = defaultdict(dict)
    complete_rows = set()

    # Create a KafkaConsumer per topic
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

    # Collect data until we have 500 complete (matched) rows
    while len(complete_rows) < 500:
        for topic in topics:
            msg_pack = consumers[topic].poll(timeout_ms=500, max_records=10)
            for _, messages in msg_pack.items():
                for msg in messages:
                    data = msg.value
                    idx = data.get("idx")
                    if idx is None:
                        continue
                    topic_key = topic.split('.')[-1]  # pin, geo, or user
                    data_by_idx[idx][topic_key] = data
                    if all(k in data_by_idx[idx] for k in ['pin', 'geo', 'user']):
                        complete_rows.add(idx)
        print(f"{len(complete_rows)} complete rows collected...")

    # Close all consumers
    for consumer in consumers.values():
        consumer.close()

    # Convert matched rows to DataFrames
    pins, geos, users = [], [], []
    for idx in list(complete_rows)[:500]:  # just to be safe
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

    Thread(target=run_webserver, daemon=True).start()
    Thread(target=run_infinite_post_data_loop, daemon=True).start()

   
    sleep(10)

    df_pin, df_geo, df_user = extract_500_messages_per_topic()
    print(df_pin.info())
    print(df_geo.info())
    print(df_user.info())

    #####

    