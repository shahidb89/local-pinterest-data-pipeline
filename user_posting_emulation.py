import yaml
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
from json import dumps, loads
import sqlalchemy
from sqlalchemy import text
import psycopg2

from fastapi import FastAPI, Request
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
import uvicorn
from threading import Thread

from collections import defaultdict

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
        sleep(random.randint(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            try:
                pin_result = dict(connection.execute(
                    text(f"SELECT * FROM pinterest_data LIMIT 1 OFFSET {random_row}")
                ).mappings().first())

                geo_result = dict(connection.execute(
                    text(f"SELECT * FROM geolocation_data LIMIT 1 OFFSET {random_row}")
                ).mappings().first())

                user_result = dict(connection.execute(
                    text(f"SELECT * FROM user_data LIMIT 1 OFFSET {random_row}")
                ).mappings().first())

                #Post geo data
                requests.post(
                    "http://localhost:8000/send_data?topic=pin_data.geo",
                    data=dumps(geo_result, default=str),
                    headers={"Content-Type": "application/json"}
                )

                #Post user data
                requests.post(
                    "http://localhost:8000/send_data?topic=pin_data.user",
                    data=dumps(user_result, default=str),
                    headers={"Content-Type": "application/json"}
                )

                #Post pin data
                requests.post(
                    "http://localhost:8000/send_data?topic=pin_data.pin",
                    data=dumps(pin_result, default=str),
                    headers={"Content-Type": "application/json"}
                )

            except Exception as e:
                print(f"[ERROR] Failed to fetch or post data: {e}")
            

            # TODO Milestone 4 Task 1
            # Note - This will work with the POST function
            # FastAPI will infer what the payload and topic are automatically!
            # Naturally this is only 1 topic you will have to replicate this for the other 2.
            # response_geo = requests.post("http://localhost:8000/send_data?topic=pin_data.geo",  data=json.dumps(geo_result, default=str))
     
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

if __name__ == "__main__":

    Thread(target=run_webserver, daemon=True).start()
    Thread(target=run_infinite_post_data_loop, daemon=True).start()

    while True:
        sleep(1)

    # # TODO When you arrive at specific Milestones/Tasks uncomment the code associated!


    # # TODO Milestone 4 Task 2
    # api = FastAPI()


    # # TODO Milestone 4 Task 1
    # producer = KafkaProducer(
    #     bootstrap_servers="localhost:9092",
    #     # TODO uncomment and fill in below fields:
    #     client_id= "Pinterest data producer", # Name the producer apprioprately
    #     value_serializer= lambda pinmessage: dumps(pinmessage).encode("utf-8") # Hint - Serialise Python dict to bytes using JSON and encode using utf-8
    # )


    # # TODO Milestone 4 task 1
    # consumer = KafkaConsumer(
    #     "pin_data.geo",
    #     bootstrap_servers="localhost:9092",
    #     # TODO uncomment and fill in below fields:
    #     auto_offset_reset="earliest",
    #     value_deserializer= lambda message: loads(message)# Hint - Load the JSON
    # )


    # # TODO Milestone 4 task 2
    # @api.get("/get_data")
    # def retrieve_data():
    #     msg = next(consumer)
    #     return msg


    # # TODO Milestone 4 task 2
    # @api.post("/send_data")
    # def send_data(payload: dict, topic: str):
    #     print(payload)
    #     # TODO send the data using the Kafka Producer
        
        
    #     producer.flush() # This line ensures all messages are sent to Kafka broker
        

    # # TODO Milestone 4 task 2
    # def run_webserver():
    #     uvicorn.run(app=api, host="localhost", port=8000)
    #     return 
    

    # # TODO Milestone 4 task 2
    # Thread(target=run_webserver, daemon=True).start()


    # run_infinite_post_data_loop()
    # print('Working')
    
    


