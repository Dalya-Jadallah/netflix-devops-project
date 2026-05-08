# Netflix DevOps + DataOps + MLOps Pipeline

A real-time event streaming pipeline that mimics Netflix's recommendation system infrastructure. This project demonstrates how Netflix processes billions of user interactions (clicks, plays, likes) to power personalized recommendations.

## 🎯 Project Overview

This pipeline ingests user clickstream events through a REST API, streams them through Apache Kafka, and prepares data for machine learning model training. It showcases three key engineering disciplines:

- **DevOps**: Containerized microservices with Docker Compose
- **DataOps**: Real-time event streaming with Kafka
- **MLOps**: Structured data pipeline ready for model training

## 🏗️ Architecture

**Event Flow:** `Client (curl) → Flask API → Kafka → Data Lake (Storage)`

- **Port 5000**: Flask receives HTTP POST events
- **Port 9092**: Kafka streams messages to consumers
- **User events**: JSON format with user_id, movie_id, event_type, timestamp

## 🛠️ Technologies Used

| Category      | Tools                                 |
|---------------|---------------------------------------|
| **DevOps**    | Docker, Docker Compose, Git           |
| **DataOps**   | Apache Kafka, Python, Flask           |
| **MLOps**     | JSON event format, Pandas (data prep) |
| **Languages** | Python 3.9+                           |

## 📋 Prerequisites

Before running this project, ensure you have:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Git](https://git-scm.com/) installed
- Python 3.9+ (for local testing)

## 🚀 Quick Start

## 1. Clone the Repository

git clone https://github.com/Dalya-Jadallah/netflix-devops-project.git
cd netflix-devops-project

## 2. Start the Pipeline
docker-compose up --build

This launches three containers:

Zookeeper (coordination service for Kafka)
Kafka (message broker)
Flask API (event ingestion service)

## 3. Send Test Events
Open a new terminal and run:

# Health check
curl http://localhost:5000/health

# Send a single event
curl -X POST http://localhost:5000/event \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user_123","movie_id":"movie_456","event_type":"play","session_id":"session_789"}'

# Send multiple events at once
curl -X POST http://localhost:5000/events/batch \
  -H "Content-Type: application/json" \
  -d '[{"user_id":"user_1","movie_id":"movie_10","event_type":"click"},{"user_id":"user_2","movie_id":"movie_20","event_type":"like"}]'

## 4. Verify Events in Kafka
docker exec -it netflix-devops-project-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic user-clicks \
  --from-beginning

You should see your JSON events appear with timestamps.

📁 Project Structure
text
netflix-devops-project/
├── app.py                 # Flask API (event receiver)
├── Dockerfile             # Container definition
├── docker-compose.yml     # Multi-container orchestration
├── requirements.txt       # Python dependencies
├── generate_events.py     # Event simulator (testing)
└── README.md              # This file

🔧 API Endpoints
Method	Endpoint	Description
GET	/health	Health check
POST	/event	Send a single user event
POST	/events/batch	Send multiple events at once

Event Schema
{
  "user_id": "string",
  "movie_id": "string", 
  "event_type": "click|play|pause|like",
  "session_id": "string",
  "processed_timestamp": "auto-added by system"
}

👥 Team Members
Dalya Jadallah
Layan Abu Ghazal
Dana Shublaq

📚 Course Information
Course: Special Topics 1 in Data Science and AI
Professor: Dr. Omar Alqawasmeh
Date: May 2026