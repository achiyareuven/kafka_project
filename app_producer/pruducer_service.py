from fastapi import FastAPI, HTTPException
from app_producer.manager import Manager
import uvicorn


app = FastAPI()
manager = Manager()



@app.get("/")
def read_root():
    return {"message": "Welcome to the Producer Service!"}


@app.get("/send_all_data")
def send_all_data():
    try:
        s1 = manager.send_not_interesting()
        s2 = manager.send_interesting()
        return {"ok": True, "sent_not_interesting": s1, "sent_interesting": s2}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/try")
def get():
    return manager.interesting_data[1]


if __name__ == '__main__':
    uvicorn.run(app,host="0.0.0.0",port=8004)








