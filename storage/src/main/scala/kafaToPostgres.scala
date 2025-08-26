package storage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import processing.config.ProcessingConfig
object KafkaToPostgres {

    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("Kafka to PostgreSQL")
        .master("local[*]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      // Read from Kafka
      val kafkaDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", ProcessingConfig.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ProcessingConfig.OUTPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

      //  Define JSON schema
      val jsonSchema = new StructType()
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

      //  Parse JSON
      val processedDF: DataFrame = kafkaDF
        .withColumn("data", from_json(col("value"), jsonSchema))
        .select("data.*")

      //  Write to PostgreSQL
      val query = processedDF.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.write
            .format("jdbc")
            .option("url", ProcessingConfig.POSTGRES_URL)
            .option("dbtable", ProcessingConfig.POSTGRES_TABLE)
            .option("user", ProcessingConfig.POSTGRES_USER)
            .option("password", ProcessingConfig.POSTGRES_PASSWORD)
            .mode("append")
            .save()
        }
        .option("checkpointLocation", "/tmp/kafka-to-postgres-checkpoint")
        .start()

      query.awaitTermination()
    }


}
