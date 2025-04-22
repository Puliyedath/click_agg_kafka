import os
import sys
import json

from confluent_kafka import Consumer, TopicPartition, KafkaException

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
        consumer.assign([TopicPartition(TOPIC, PARTITION_ID)])
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



