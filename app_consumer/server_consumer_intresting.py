from fastapi import FastAPI,HTTPException
from app_consumer.consumer import Consumer
from dal import MongoWriter
import os
import uvicorn

app = FastAPI()

mongo_writer =None
consumer_interested = None


@app.get("/write_to_mongo")
def write_to_mongo():
    try:
        global consumer_interested ,mongo_writer
        mongo_writer = MongoWriter(None, None, os.getenv("MONGO_COL_INTERESTED", "interested"))
        consumer_interested = Consumer(os.getenv("TOPIC_INTERESTED","interesting"), mongo_writer,os.getenv("GROUP_ID_INTERESTED","group_interesting"))
        consumer_interested.write_to_mongo()
        return {"ok": True, "message": "Data written to MongoDB"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/read_from_mongo")
def read_from_mongo():
    try:
        global mongo_writer
        if mongo_writer is None:
            mongo_writer = MongoWriter(None, None, os.getenv("MONGO_COL_INTERESTED", "interested"))
        data = list(mongo_writer.col.find({}, {"_id": 0}))
        return {"ok": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"message": "Welcome to the interested Service!"}


if __name__ == '__main__':
    uvicorn.run(app,host="0.0.0.0",port=8006)





