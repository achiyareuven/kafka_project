
import os
import pymongo
from datetime import datetime


class MongoWriter:
    def __init__(self, uri= None, db_name = None, col_name = None):
        self.uri = uri or os.getenv("MONGO_CONN")
        if not self.uri:
            raise ValueError("MONGO_CONN missing")
        self.client = pymongo.MongoClient(self.uri)
        self.db = self.client[db_name or os.getenv("MONGO_DB", "news_db")]
        self.col = self.db[col_name or os.getenv("MONGO_COL_INTERESTED", "interested")]

    def insert_event(self,message,topic,ingested_at):
        doc = {
            "payload": message,
            "topic": topic,
            "ingested_at":ingested_at

        }
        return self.col.insert_one(doc)
