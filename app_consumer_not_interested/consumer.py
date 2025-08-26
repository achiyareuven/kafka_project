from kafka import KafkaConsumer
import pymongo
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from dal import MongoWriter

load_dotenv()


class Consumer:
    def __init__(self,topic,writer:MongoWriter,group_id):
        self.writer = writer
        self.topic = topic
        self.consumer = KafkaConsumer(topic,
                                 group_id=group_id,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                 bootstrap_servers=['localhost:9092'],
                                 consumer_timeout_ms=10000,
                                 auto_offset_reset = "earliest")

    def write_to_mongo(self):
        for message in self.consumer:
            ingested_at = datetime.now()
            self.writer.insert_event( message.value,self.topic,ingested_at)
            print("Message inserted into MongoDB")




