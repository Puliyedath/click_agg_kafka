from contextlib import asynccontextmanager
import json
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Producer
import redis

producer = None
r = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    global r
    print("Starting up...")
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9094"),
        "acks": "all"
    })
    r = redis.Redis(host=os.getenv("REDIS_HOST", "counter-redis-db"), port=os.getenv("REDIS_PORT", 6379), password=os.getenv("REDIS_PASSWORD", "redis") )
    print(f"Redis connected to {os.getenv('REDIS_HOST', 'counter-redis-db')}:{os.getenv('REDIS_PORT', 6379)}")
    yield
    print("Shutting down...flush producer")
    producer.flush()  
    r.close()

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


async def send_kafka_message(request: Request, event_type: str):
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
            "event": event_type
        }
        producer.produce(
            topic="user-likes-counter",
            key=video_id,
            value=json.dumps(payload).encode("utf-8"),
            callback=acked
        )
        producer.poll(10000)
        producer.flush()

@app.post("/like")
async def like(request: Request):
    await send_kafka_message(request, "like")


@app.post("/dislike")
async def dislike(request: Request):
    await send_kafka_message(request, "dislike")

@app.get("/likes/{video_id}")
async def likes(video_id: str):
    print("hareesh=", r)
    return {"message": r.hgetall(video_id), "video_id": video_id}
    