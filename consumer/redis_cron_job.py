# add a cron job that reads the redis sorted set and publishes the same to pubsub channel

import redis
import time
import json
import os
# Redis connection
r = redis.Redis(host=os.getenv("REDIS_HOST", "counter-redis-db"), port=os.getenv("REDIS_PORT", 6379), password=os.getenv("REDIS_PASSWORD", "redis") )# add a cron job that reads the redis sorted set and publishes the same to pubsub channel

SORTED_SET_KEY = 'video_likes_counter'  # Replace with your sorted set key
PUBSUB_CHANNEL = 'video_likes_counter'  # Replace with your pubsub channel

def publish_sorted_set():
    # Get all elements from the sorted set
    items = r.zrange(SORTED_SET_KEY, 0, 4, withscores=True)
    if items:
        print("Hareesh items=", items)
        # Prepare the message (convert bytes to string)
        message = [
            {'member': member.decode('utf-8'), 'score': score}
            for member, score in items
        ]
        # Publish to pubsub channel as JSON
        r.publish(PUBSUB_CHANNEL, json.dumps(message))
        print(f"Published {len(items)} items to channel '{PUBSUB_CHANNEL}'")
    else:
        print("No items in sorted set.")

if __name__ == "__main__":
    try:
        while True:
            print("Hareesh publishing sorted set cron job running >>>>>")
            publish_sorted_set()
            time.sleep(5)  # Run every 60 seconds (adjust as needed)
    except Exception as e:
        print(f"Error in publish_sorted_set: {e}")
        raise e
    finally:
        r.close()



