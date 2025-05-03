import asyncio
from contextlib import asynccontextmanager
import json
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Producer
from fastapi.responses import StreamingResponse
import redis.asyncio as redis

producer = None
r = None
pubsub = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    global r
    global pubsub
    print("Starting up...")
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9094"),
        "acks": "all"
    })
    r = redis.Redis(host=os.getenv("REDIS_HOST", "counter-redis-db"), port=os.getenv("REDIS_PORT", 6379), password=os.getenv("REDIS_PASSWORD", "redis") )
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    print(f"Redis connected to {os.getenv('REDIS_HOST', 'counter-redis-db')}:{os.getenv('REDIS_PORT', 6379)}")
    yield
    print("Shutting down...flush producer")
    producer.flush()  
    await r.close()

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

@app.get("/top-videos")
async def top_videos():
    likes = await r.zrange("video_likes_counter", 0, 4, withscores=True)
    dislikes = await r.zrange("video_dislikes_counter", 0, 4, withscores=True)
    # get the video_id from the likes and dislikes
    likes_video_ids = [int(like[0]) for like in likes]
    dislikes_video_ids = [int(dislike[0]) for dislike in dislikes]
    # get the video details from redis
    print("likes_video_ids=", likes_video_ids)
    print("dislikes_video_ids=", dislikes_video_ids)
    pipeline = await r.pipeline()
    video_ids = likes_video_ids + dislikes_video_ids
    for video_id in video_ids:
        await pipeline.hgetall(video_id)
    video_details = await pipeline.execute()
    return {
        "video_details": {
            video_id: video_detail for video_id, video_detail in zip(video_ids, video_details)}
    }

@app.get("/sse")
async def sse(request: Request):
    global pubsub
    await pubsub.subscribe("video_likes_counter")
    async def generate_sse_events(request: Request):
        try:
             async for message in pubsub.listen():
                if (await request.is_disconnected()):
                    break
                if message is None or message["type"] != "message":
                    continue
                data = json.loads(message.get("data").decode("utf-8"))
                video_ids = [int(d['member']) for d in data]
                pipeline = await r.pipeline()
                for video_id in video_ids:
                    await pipeline.hgetall(video_id)
                video_details = await pipeline.execute()

                payload = {
                    int(video_id): {
                        k.decode("utf-8"): v.decode("utf-8")
                        for k, v in video_detail.items()
                    }
                    for video_id, video_detail in zip(video_ids, video_details)
                }
                yield f"data: {json.dumps(payload)}\n\n"
        except Exception as e:
            print(f"Error in generate_sse_events: {e}")
            raise e
        finally:
            await pubsub.unsubscribe("video_likes_counter")
            await pubsub.close()

    return StreamingResponse(
        generate_sse_events(request), media_type="text/event-stream")

    