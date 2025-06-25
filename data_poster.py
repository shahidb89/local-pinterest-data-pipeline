"""
data_poster.py

This script sets up a FastAPI web server to send Pinterest-style data from a PostgreSQL database
to Kafka topics. It simulates a real-time data ingestion pipeline and continuously posts
records to different Kafka topics (pin, geo, and user) via HTTP endpoints.

Components:
- AWSDBConnector: Loads credentials from YAML and creates a SQLAlchemy database engine.
- run_infinite_post_data_loop: Continuously fetches random records and posts them.
- FastAPI routes: `/send_data` to produce to Kafka; `/get_data` to consume one message.
- Threaded startup for the server and data posting loop.

Author: Shahid Hadi
"""

import yaml
import requests
import random
import sqlalchemy
import psycopg2
from json import dumps, loads
from sqlalchemy import text
from kafka import KafkaConsumer
from time import sleep
from fastapi import FastAPI
from kafka.producer import KafkaProducer
from threading import Thread
import uvicorn

random.seed(100)


class AWSDBConnector:
    """
    A class to connect to AWS PostgreSQL database using credentials from a YAML file.
    """

    @staticmethod
    def read_db_creds():
        """
        Reads database credentials from `db_creds.yaml`.

        Returns:
            dict: A dictionary with HOST, USER, PASSWORD, DATABASE, and PORT.
        """
        with open('db_creds.yaml', 'r') as db:
            db_creds_dict = yaml.load(db, Loader=yaml.SafeLoader)
        return db_creds_dict

    def __init__(self):
        """
        Initializes the connection object with credentials.
        """
        db_creds_dict = AWSDBConnector.read_db_creds()
        self.HOST = db_creds_dict['HOST']
        self.USER = db_creds_dict['USER']
        self.PASSWORD = db_creds_dict['PASSWORD']
        self.DATABASE = db_creds_dict['DATABASE']
        self.PORT = db_creds_dict['PORT']
        
    def create_db_connector(self):
        """
        Creates a SQLAlchemy engine using PostgreSQL+psycopg2.

        Returns:
            sqlalchemy.Engine: Database connection engine.
        """
        return sqlalchemy.create_engine(
            f"postgresql+psycopg2://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"
        )


# Kafka & FastAPI setup
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
    """
    Continuously retrieves and sends random Pinterest-style data rows to FastAPI endpoints,
    which then send the data to Kafka topics.
    """
    engine = new_connector.create_db_connector()
    while True:
        sleep(0.05)
        random_row = random.randint(0, 11000)
        with engine.connect() as connection:
            try:
                pin_result = dict(connection.execute(
                    text(f"SELECT * FROM pinterest_data LIMIT 1 OFFSET {random_row}")
                ).mappings().first())
                idx = pin_result['idx']

                geo_result = dict(connection.execute(
                    text("SELECT * FROM geolocation_data WHERE idx = :idx"),
                    {"idx": idx}
                ).mappings().first())

                user_result = dict(connection.execute(
                    text("SELECT * FROM user_data WHERE idx = :idx"),
                    {"idx": idx}
                ).mappings().first())

                if geo_result and user_result:
                    for topic, data in {
                        "pin_data.geo": geo_result,
                        "pin_data.user": user_result,
                        "pin_data.pin": pin_result
                    }.items():
                        requests.post(
                            f"http://localhost:8000/send_data?topic={topic}",
                            data=dumps(data, default=str),
                            headers={"Content-Type": "application/json"}
                        )
                else:
                    print(f"[WARN] Missing geo/user data for idx: {idx}")
            except Exception as e:
                print(f"[ERROR] Failed to fetch or post data: {e}")


@app.post("/send_data")
async def send_data(payload: dict, topic: str):
    """
    FastAPI route to send a JSON payload to a specified Kafka topic.

    Args:
        payload (dict): The JSON-formatted data to send.
        topic (str): The Kafka topic name.

    Returns:
        dict: Success or error status message.
    """
    try:
        producer.send(topic, value=payload)
        producer.flush()
        return {"status": "Message sent", "topic": topic}
    except Exception as e:
        return {"error": str(e)}


@app.get("/get_data")
def retrieve_data():
    """
    FastAPI route to retrieve one message from Kafka consumer.

    Returns:
        dict: Message topic and contents.
    """
    msg = next(consumer)
    return {"topic": msg.topic, "data": msg.value}


def run_webserver():
    """
    Starts the FastAPI app using Uvicorn on localhost.
    """
    uvicorn.run(app, host="localhost", port=8000)


if __name__ == "__main__":
    """
    Launch the web server and infinite data posting loop in parallel threads.
    """
    Thread(target=run_webserver, daemon=True).start()
    Thread(target=run_infinite_post_data_loop, daemon=True).start()
    while True:
        sleep(60)
