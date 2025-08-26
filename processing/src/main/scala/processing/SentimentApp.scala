package processing


import org.apache.spark.sql.SparkSession
import processing.config.ProcessingConfig
import processing.ml.SentimentAnalyzer
import processing.stream.KafkaStreamProcessor

import scala.util.Try

object SentimentApp {

  def main(args: Array[String]): Unit = {
    println("🚀 Starting Reddit Sentiment Analysis Pipeline...")

    // Initialize Spark Session with optimized settings
    val spark = SparkSession.builder()
      .appName(ProcessingConfig.APP_NAME)
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", ProcessingConfig.CHECKPOINT_LOCATION)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      println("📈 Training sentiment analysis model...")
      val sentimentModel = SentimentAnalyzer.createSentimentModel(spark)

      println("📡 Setting up Kafka streams...")
      val postsStream = KafkaStreamProcessor.readPostsStream(spark)
      val commentsStream = KafkaStreamProcessor.readCommentsStream(spark)

      println("⚙️ Processing unified stream...")
      val processedStream = KafkaStreamProcessor.processUnifiedStream(
        postsStream, commentsStream, sentimentModel, spark
      )

      println("📤 Setting up output streams...")
      // Write to Kafka for Elasticsearch
      val kafkaQuery = KafkaStreamProcessor.writeToKafka(processedStream)

      // Write to console for monitoring
      val consoleQuery = KafkaStreamProcessor.writeToConsole(processedStream)

      println("✅ Pipeline started successfully!")
      println("📊 Monitor the console output for real-time sentiment analysis results")
      println("🔗 Processed data is being sent to Kafka topic: processed-sentiment")
      println("⏹️  Press Ctrl+C to stop the pipeline")

      // Handle graceful shutdown
      sys.addShutdownHook {
        println("🛑 Shutting down pipeline...")
        Try(kafkaQuery.stop())
        Try(consoleQuery.stop())
        spark.stop()
        println("✅ Pipeline stopped successfully!")
      }

      // Wait for termination
      kafkaQuery.awaitTermination()

    } catch {
      case e: Exception =>
        println(s"❌ Error in streaming pipeline: ${e.getMessage}")
        e.printStackTrace()

        // Log error details
        println("🔍 Error Details:")
        println(s"  - Message: ${e.getMessage}")
        println(s"  - Cause: ${Option(e.getCause).map(_.getMessage).getOrElse("Unknown")}")

    } finally {
      println("🧹 Cleaning up resources...")
      spark.stop()
    }
  }
}
