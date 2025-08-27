package storage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import processing.config.ProcessingConfig

object KafkaToPostgres {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Kafka to PostgreSQL (Processed + DLQ)")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    val processedKafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ProcessingConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ProcessingConfig.OUTPUT_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val processedSchema = new StructType()
      .add("id", StringType)
      .add("original_content", StringType)
      .add("cleaned_text", StringType)
      .add("content_type", StringType)
      .add("author", StringType)
      .add("subreddit", StringType)
      .add("score", IntegerType)
      .add("created_utc", LongType)
      .add("parent_id", StringType)
      .add("sentiment", StringType)
      .add("sentiment_score", DoubleType)
      .add("confidence", DoubleType)
      .add("text_length", IntegerType)
      .add("engagement_score", IntegerType)
      .add("processing_timestamp", TimestampType)
      .add("model_version", StringType)

    val processedDF: DataFrame = processedKafkaDF
      .withColumn("data", from_json(col("value"), processedSchema))
      .select("data.*")

    val processedQuery = processedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("url", ProcessingConfig.POSTGRES_URL)
          .option("dbtable", ProcessingConfig.POSTGRES_TABLE) // processed_reddit
          .option("user", ProcessingConfig.POSTGRES_USER)
          .option("password", ProcessingConfig.POSTGRES_PASSWORD)
          .mode("append")
          .save()
      }
      .option("checkpointLocation", "/tmp/kafka-to-postgres-checkpoint/processed")
      .start()


    val dlqKafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ProcessingConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ProcessingConfig.DLQ_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val dlqSchema = new StructType()
      .add("original_message", StringType)
      .add("error_type", StringType)
      .add("error_message", StringType)
      .add("timestamp", LongType)
      .add("retry_count", IntegerType)
      .add("topic", StringType)

    val dlqDF: DataFrame = dlqKafkaDF
      .withColumn("data", from_json(col("value"), dlqSchema))
      .select("data.*")
      .withColumn("error_timestamp", to_timestamp(col("timestamp")))

    val dlqQuery = dlqDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("url", ProcessingConfig.POSTGRES_URL)
          .option("dbtable", ProcessingConfig.POSTGRES_DLQ_TABLE) // reddit_dlq
          .option("user", ProcessingConfig.POSTGRES_USER)
          .option("password", ProcessingConfig.POSTGRES_PASSWORD)
          .mode("append")
          .save()
      }
      .option("checkpointLocation", "/tmp/kafka-to-postgres-checkpoint/dlq")
      .start()
    spark.streams.awaitAnyTermination()
  }
}
