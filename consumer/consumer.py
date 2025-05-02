import os
import sys
import json
import redis

from confluent_kafka import Consumer, KafkaException

r = redis.Redis(host=os.getenv("REDIS_HOST", "counter-redis-db"), port=os.getenv("REDIS_PORT", 6379), password=os.getenv("REDIS_PASSWORD", "redis") )

def update_video_action(video_id, action) -> None:
    key = "like" if action == "like" else "dislike"
    pipeline = r.pipeline()
    pipeline.hincrby(video_id, key, 1)
    pipeline.execute()

    video_obj = r.hgetall(video_id)
    # Step 3: Convert byte values to int => redis stores values as bytes strings
    like_count = int(video_obj.get(b"like", 0))
    dislike_count = int(video_obj.get(b"dislike", 0))
    print("Hareesh like_count=", like_count)
    print("Hareesh dislike_count=", dislike_count)

    # Step 4: Update the sorted sets
    pipeline = r.pipeline()
    pipeline.zadd("video_likes_counter", {video_id: like_count})
    pipeline.zadd("video_dislikes_counter", {video_id: dislike_count})
    pipeline.execute()

def main():
    try:

        PARTITION_ID = int(os.getenv("PARTITION_ID", 0))  # default partition 0 if no id is provided
        TOPIC = os.getenv("TOPIC", "user-likes-counter")
        BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9094")
        GROUP_ID = os.getenv("GROUP_ID", "consumer-group")

        # Configure logging to stdout
        # print(f"Starting consumer with configuration:")
        # print(f"  Partition ID: {PARTITION_ID}")
        # print(f"  Topic: {TOPIC}")
        # print(f"  Bootstrap Servers: {BOOTSTRAP_SERVERS}")
        # print(f"  Group ID: {GROUP_ID}")

        conf = {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }

        print(f"Creating consumer...")
        consumer = Consumer(conf)
        print(f"Consumer created successfully")
        
        # Assign to specific partition
        # consumer.assign([TopicPartition(TOPIC, PARTITION_ID)])
        consumer.subscribe([TOPIC])
        print(f"Assigned to partition {PARTITION_ID}")
        
        print(f"Starting to consume messages...")
        
        while True:
            print("Polling for messages...")
            try:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Error: {msg.error()}")
                    continue
                    
                # Try to parse the message as JSON
                try:
                    message_value = msg.value().decode('utf-8')
                    message_data = json.loads(message_value)
                    print(f"Received message: {message_data}")
                    # {video_id: {like: 1, dislike: 0}}
                    # video_likes_counter 
                    # video_dislikes_counter
                    update_video_action(message_data["video_id"], message_data["event"])
                    # r.hincrby(message_data["video_id"], message_data["event"], 1)
                    print(f"Updated Redis for video {message_data['video_id']} with event {message_data['event']}")
                except json.JSONDecodeError:
                    print(f"Received non-JSON message: {message_value}")
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    continue
                    
                # Commit the message
                try:
                    consumer.commit(msg)
                    print(f"Message committed successfully")
                except KafkaException as e:
                    print(f"Error committing message: {str(e)}")
                    
            except Exception as e:
                print(f"Error in message loop: {str(e)}")
                continue
                
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
    finally:
        try:
            consumer.close()
            print("Consumer closed successfully")
        except:
            pass

if __name__ == "__main__":
    main()



