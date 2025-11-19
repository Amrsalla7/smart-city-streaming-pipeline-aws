
# 🏙️ Smart City Real-Time Data Streaming Pipeline

A comprehensive real-time data processing system that simulates smart city operations using modern data engineering technologies. This project demonstrates a complete IoT data pipeline from vehicle simulation to real-time analytics and storage.

## 🚀 Overview

This system simulates a smart city environment where vehicles generate multiple data streams as they travel from London to Birmingham. The pipeline processes real-time data for traffic monitoring, weather conditions, GPS tracking, and emergency incidents using a scalable microservices architecture.

## 🏗️ Architecture

```
Data Generation → Kafka → Spark Streaming → AWS S3 → Analytics
     (Python)     (Broker)  (Structured      (Data Lake)
                              Streaming)
```

## 🛠️ Tech Stack

- **Data Generation**: Python, Confluent Kafka
- **Message Broker**: Apache Kafka with ZooKeeper
- **Stream Processing**: Apache Spark Structured Streaming
- **Storage**: AWS S3 (Data Lake)
- **Containerization**: Docker & Docker Compose
- **Infrastructure**: Multi-container microservices architecture

## 📊 Data Streams

The system processes 5 real-time data streams:
-  **Vehicle Data** - Vehicle telemetry, specifications, and status
-  **GPS Data** - Real-time location, speed, and direction
-  **Traffic Data** - Camera feeds and traffic conditions
-  **Weather Data** - Real weather conditions from OpenWeatherMap API
-  **Emergency Data** - Incident reports and emergency services data

## 🎯 Key Features

### 🔄 Real-time Data Pipeline
- **High-throughput** data ingestion using Kafka
- **Fault-tolerant** stream processing with Spark
- **Scalable** microservices architecture
- **Exactly-once** processing semantics

### 🌐 Real API Integration
- **OpenWeatherMap API** for real weather data
- **TomTom Traffic API** for live traffic conditions
- **OpenStreetMap API** for emergency service locations
- **Graceful fallback** to synthetic data when APIs are unavailable

### 📈 Advanced Analytics
- **Structured Streaming** with micro-batch processing
- **Windowed operations** and watermarks for late data
- **Parquet format** for efficient columnar storage
- **Checkpointing** for fault tolerance and recovery

### 🐳 Containerized Deployment
- **Multi-service** Docker Compose setup
- **Custom Spark image** with AWS S3 dependencies
- **Service discovery** and network isolation
- **Health checks** and dependency management

## 📁 Project Structure

```
smart-city-streaming/
├── jobs/
│   ├── main.py              # Data producer with real APIs
│   ├── spark-city.py        # Spark streaming application
│   └── config.py           # Configuration management
├── docker-compose.yml      # Multi-service orchestration
├── Dockerfile             # Custom Spark image
├── requirements.txt       # Python dependencies
└── README.md             # Project documentation
```

## 🚦 Quick Start

### Prerequisites
- Docker & Docker Compose
- AWS Account with S3 bucket
- Python 3.8+

### Installation
```bash
# 1. Clone repository
git clone https://github.com/yourusername/smart-city-streaming.git
cd smart-city-streaming

# 2. Start services
docker-compose up -d

# 3. Generate data
python jobs/main.py

# 4. Run streaming processing
docker exec -it spark-master spark-submit /jobs/spark-city.py
```

## 🔧 Configuration

### Environment Variables
```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# AWS S3 Configuration
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_KEY=your_secret_key

# API Keys
OPENWEATHER_API_KEY=your_weather_api_key
TOMTOM_API_KEY=your_traffic_api_key
```

### Kafka Topics
- `vehicle_data` - Vehicle telemetry and specifications
- `gps_data` - Location and movement data
- `traffic_data` - Traffic conditions and camera feeds
- `weather_data` - Weather conditions and forecasts
- `emergency_data` - Incident reports and emergencies

## 📈 Monitoring

### Spark UI
Accessible at `http://localhost:9090` for:
- Streaming query progress
- Batch processing statistics
- Worker node status

### Kafka Monitoring
```bash
# List topics
docker exec broker kafka-topics --list --bootstrap-server localhost:9092

# Monitor messages
docker exec broker kafka-console-consumer --topic vehicle_data --from-beginning
```

## 🎓 Learning Outcomes

This project demonstrates:
- **Real-time stream processing** with Apache Spark
- **Microservices architecture** with Docker
- **Message queue patterns** with Kafka
- **Cloud storage integration** with AWS S3
- **API integration** for real-world data
- **Data pipeline design** and optimization
- **Container orchestration** and networking

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for:
- Additional data sources
- Enhanced analytics
- Improved monitoring
- Performance optimizations

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- **OpenWeatherMap** for weather data API
- **TomTom** for traffic data API
- **OpenStreetMap** for geographic data
- **Apache Foundation** for Spark and Kafka
- **Docker** for containerization platform

---

## 🏆 Use Cases

This project serves as an excellent foundation for:
- **Smart City IoT Platforms**
- **Real-time Traffic Management Systems**
- **Fleet Tracking and Analytics**
- **Environmental Monitoring Networks**
- **Data Engineering Learning and Demonstrations**

Perfect for developers looking to understand real-time data processing, microservices architecture, and modern data engineering practices!
