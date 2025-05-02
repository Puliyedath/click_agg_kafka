# Kafka-Redis Like Counter Application

This project implements a distributed like/dislike counter system using Kafka and Redis. It consists of a frontend React application, a FastAPI backend, and a Kafka-Redis infrastructure for handling like/dislike events.

## Architecture

- **Frontend**: React application that allows users to like/dislike videos
- **Backend**: FastAPI server that handles like/dislike requests and communicates with Kafka
- **Kafka**: Distributed event streaming platform with 2 brokers (kafka1 and kafka2)
- **Redis**: In-memory database for storing like/dislike counts
- **Consumers**: Two Python consumers that process like/dislike events and update Redis

## Prerequisites

- Docker
- Docker Compose

## Getting Started

1. Clone the repository
2. Navigate to the project directory
3. Start the stack:

   ```bash
   docker-compose up -d
   ```

4. To stop the stack:

   ```bash
   docker-compose down
   ```

## Containers

The following containers will be created:

- `counter_kafka_app_be` - Backend FastAPI service
- `counter_kafka_app_fe` - Frontend React application
- `kafka1` - First Kafka broker
- `kafka2` - Second Kafka broker
- `topic_creator` - Service to create Kafka topics
- `consumer1` - First Kafka consumer
- `consumer2` - Second Kafka consumer
- `counter-redis-db` - Redis database

## Accessing the Application

- **Frontend**: [http://localhost:5173](http://localhost:5173)
- **Backend**: [http://localhost:8000](http://localhost:8000)
- **Redis**: `localhost:6379`
- **Kafka brokers**: `localhost:9092` and `localhost:9094`

All containers are connected through a bridge network called `counter_kafka_app_network`. The setup includes health checks for critical services and proper dependency management to ensure services start in the correct order.

