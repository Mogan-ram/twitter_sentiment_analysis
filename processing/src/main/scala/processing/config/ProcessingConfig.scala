package processing.config

object ProcessingConfig {
  // Kafka Configuration
  val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  val POSTS_INPUT_TOPIC = "reddit-posts"
  val COMMENTS_INPUT_TOPIC = "reddit-comments"
  val OUTPUT_TOPIC = "processed-sentiment"

  // Spark Configuration
  val CHECKPOINT_LOCATION = "/tmp/reddit-sentiment-checkpoint"
  val PROCESSING_INTERVAL = "10 seconds"
  val MAX_OFFSETS_PER_TRIGGER = 1000

  // ML Configuration
  val MIN_CONFIDENCE_THRESHOLD = 0.3
  val MIN_TEXT_LENGTH = 10
  val MODEL_VERSION = "1.2"

  // Application Configuration
  val APP_NAME = "Reddit Sentiment Analysis Pipeline"

  //Storage Configuration
  val POSTGRES_URL = "jdbc:postgresql://localhost:5432/Reddit_data"
  val POSTGRES_USER = "postgres"
  val POSTGRES_PASSWORD = "Greeshmanth123"
  val POSTGRES_TABLE = "processed_reddit"

}
