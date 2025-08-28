# Reddit Sentiment Analysis Pipeline

## Overview
The **Reddit Sentiment Analysis Pipeline** is a real-time data processing application designed to analyze the sentiment of Reddit posts and comments. It leverages Apache Spark for stream processing, Apache Kafka for data streaming, and a hybrid sentiment analysis model combining VADER (Valence Aware Dictionary and sEntiment Reasoner) with a pre-trained Logistic Regression model. The pipeline processes streaming data from Reddit, performs sentiment analysis, and outputs results to Kafka for downstream consumption, with additional features like a Dead Letter Queue (DLQ) for error handling and real-time metrics monitoring.

This project was developed as part of a Revature training program, focusing on big data technologies and real-time data engineering. It demonstrates proficiency in Scala, Spark, Kafka, and sentiment analysis techniques.

## Features
- **Real-time Processing**: Streams Reddit posts and comments from Kafka topics (`reddit-posts`, `reddit-comments`).
- **Hybrid Sentiment Analysis**: Combines VADER lexicon-based analysis with a Logistic Regression model for improved accuracy.
- **Error Handling**: Implements a Dead Letter Queue to capture and log failed messages.
- **Real-time Metrics**: Monitors processing metrics such as sentiment distribution, engagement scores, and processing rates.
- **Scalable Architecture**: Uses Spark's streaming capabilities with optimized configurations for adaptive partitioning and skew handling.
- **Console Monitoring**: Provides detailed console output for real-time debugging and monitoring.
- **Modular Design**: Organized into ingestion, processing, storage, visualization, and utility modules.

## Project Structure
The project is organized into multiple modules, each with a specific responsibility:

```
RedditSentimentPipeline/
├── ingestion/              # Handles data ingestion from Reddit API to Kafka
├── processing/             # Core processing logic with Spark and sentiment analysis
│   ├── config/             # Configuration settings (e.g., Kafka, Spark)
│   ├── evaluation/         # Model evaluation and metrics
│   ├── ml/                 # Machine learning and sentiment analysis logic
│   ├── monitor/            # Monitoring and Dead Letter Queue handling
│   ├── stream/             # Kafka stream processing
│   └── utils/              # Shared utilities (e.g., text processing)
├── storage/                # Storage logic (e.g., PostgreSQL integration)
├── visualization/          # Visualization layer (e.g., Grafana integration)
├── utils/                  # Common utilities shared across modules
└── build.sbt               # SBT build configuration
```

Key files in the `processing` module:
- `SentimentApp.scala`: Main entry point for the pipeline, orchestrating Spark session and stream processing.
- `EnhancedKafkaStreamProcessor.scala`: Enhanced stream processing with error handling and DLQ.
- `KafkaStreamProcessor.scala`: Core Kafka stream reading and parsing logic.
- `PreTrainedSentimentAnalyzer.scala`: Implements hybrid sentiment analysis (VADER + ML).
- `ProcessingMetrics.scala`: Calculates real-time processing metrics.
- `DeadLetterQueue.scala`: Manages failed messages and logging.
- `SentimentAnalyzer.scala`: Basic sentiment analysis model training.
- `ModelEvaluator.scala`: Evaluates model performance with detailed metrics.
- `ProcessingConfig.scala`: Configuration settings for Kafka, Spark, and ML.

## Prerequisites
To run this project, ensure you have the following installed:
- **Java 8 or 11**
- **Scala 2.12.18**
- **Apache Spark 3.4.0**
- **Apache Kafka 3.6.0**
- **SBT 1.9.x**
- **PostgreSQL** (optional, for storage module)
- **Grafana** (optional, for visualization module)

## Setup Instructions
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/RedditSentimentPipeline.git
   cd RedditSentimentPipeline
   ```

2. **Install Dependencies**:
   Run the following command to resolve dependencies defined in `build.sbt`:
   ```bash
   sbt update
   ```

3. **Configure Kafka**:
   - Start a Kafka server on `localhost:9092` (default configuration in `ProcessingConfig.scala`).
   - Create Kafka topics:
     ```bash
     kafka-topics.sh --create --topic reddit-posts --bootstrap-server localhost:9092
     kafka-topics.sh --create --topic reddit-comments --bootstrap-server localhost:9092
     kafka-topics.sh --create --topic processed-sentiment --bootstrap-server localhost:9092
     kafka-topics.sh --create --topic reddit-dlq --bootstrap-server localhost:9092
     ```

4. **Set Up Spark**:
   - Ensure Spark is installed and configured.
   - Update `ProcessingConfig.scala` if using a different checkpoint location or Kafka bootstrap servers.

5. **(Optional) Configure Storage and Visualization**:
   - For PostgreSQL, configure connection details in the `storage` module.
   - For Grafana, set up dashboards in the `visualization` module to visualize metrics.

6. **Run the Pipeline**:
   Compile and run the application using SBT:
   ```bash
   sbt "project processing" run
   ```

## Usage
- The pipeline reads data from Kafka topics `reddit-posts` and `reddit-comments`.
- It processes the data using a hybrid sentiment analysis model (VADER + Logistic Regression).
- Results are output to the `processed-sentiment` Kafka topic in JSON format, suitable for Elasticsearch or other downstream systems.
- Failed messages are routed to the `reddit-dlq` topic.
- Real-time metrics are displayed in the console and updated every 2 minutes.
- Press `Ctrl+C` to gracefully stop the pipeline.

### Sample Output
Console output example:
```
✅ Enhanced pipeline started successfully!
================================================================================
📊 PIPELINE FEATURES:
  🎯 Hybrid Sentiment Analysis (VADER + ML Model)
  📈 Model Accuracy: 85.2%
  🎭 F1 Score: 83.7%
  🔄 Real-time Processing (Posts + Comments)
  ❌ Dead Letter Queue for Failed Messages
  📊 Real-time Metrics & Monitoring
  🎯 Confidence-based Quality Filtering
================================================================================
📡 DATA FLOW:
  Kafka Topics: reddit-posts, reddit-comments → processing → processed-sentiment
  Failed Messages → reddit-dlq
  Elasticsearch Ready Output Format
================================================================================
```

## Dependencies
Key dependencies (see `build.sbt` for full list):
- **Apache Spark**: `spark-core`, `spark-sql`, `spark-streaming`, `spark-mllib`, `spark-sql-kafka-0-10` (v3.4.0)
- **Apache Kafka**: `kafka` (v3.6.0)
- **JSON4s**: `json4s-jackson`, `json4s-core` (v3.7.0-M11)
- **Stanford CoreNLP**: `stanford-corenlp` (v4.5.0)
- **Spark NLP**: `spark-nlp` (v5.1.4)
- **SLF4J and Logback**: For logging

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m "Add your feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Create a pull request.


## Acknowledgments
- Developed as part of Revature's Java Full Stack training program.
- Inspired by real-world data engineering challenges in social media analytics.
- Thanks to the open-source communities behind Spark, Kafka, and VADER.