from contextlib import asynccontextmanager
import json
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Producer

producer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    print("Starting up...")
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9094"),
        "acks": "all"
    })
    yield
    print("Shutting down...flush producer")
    producer.flush()    

app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:5173",
    "localhost:5173",
    "http://localhost:3000",
    "localhost:3000"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)



@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to your todo list."}




@app.post("/like")
async def like(request: Request):
    def acked(err, msg):
        if err is not None:
            print(f"Failed to deliver message: {str(err)}")
            return {"message": "Failed to deliver message: %s" % str(err)}
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
            return {"message": f"Message delivered to {msg.topic()} [{msg.partition()}]"}
            
    data = await request.json()
    video_id = data.get("video_id")
    assert video_id is not None, "Video ID is required"
    user_id = data.get("user_id")
    assert user_id is not None, "User ID is required"
    payload = {
        "video_id": video_id,
        "user_id": user_id,
        "event": "like"
    }
    producer.produce(
        topic="user-likes-counter",
        key=video_id,
        value=json.dumps(payload).encode("utf-8"),
        callback=acked
    )
    producer.flush()


@app.post("/dislike")
async def dislike(request: Request):
    def acked(err, msg):
        if err is not None:
            print(f"Failed to deliver message: {str(err)}")
            return {"message": "Failed to deliver message: %s" % str(err)}
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
            return {"message": f"Message delivered to {msg.topic()} [{msg.partition()}]"}
            
    data = await request.json()
    video_id = data.get("video_id")
    assert video_id is not None, "Video ID is required"
    user_id = data.get("user_id")
    assert user_id is not None, "User ID is required"
    payload = {
        "video_id": video_id,
        "user_id": user_id,
        "event": "dislike"
    }
    producer.produce(
        topic="user-likes-counter",
        key=video_id,
        value=json.dumps(payload).encode("utf-8"),
        callback=acked
    )
    producer.flush()
