package processing.monitor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import processing.config.ProcessingConfig

object DeadLetterQueue {

  case class FailedMessage(
                            original_message: String,
                            error_type: String,
                            error_message: String,
                            timestamp: Long,
                            retry_count: Int,
                            topic: String
                          )

  /**
   * Handle failed messages and route to dead letter queue
   */
  def handleFailedMessages(failedStream: DataFrame): StreamingQuery = {
    val dlqOutput = failedStream
      .select(
        col("original_message"),
        col("error_type"),
        col("error_message"),
        unix_timestamp().alias("timestamp"),
        lit(0).alias("retry_count"),
        col("source_topic").alias("topic")
      )
      .select(to_json(struct(col("*"))).alias("value"))

    dlqOutput.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ProcessingConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("topic", "reddit-dlq")
      .option("checkpointLocation", s"${ProcessingConfig.CHECKPOINT_LOCATION}/dlq")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
  }

  /**
   * Log failed messages to console for monitoring
   */
  def logFailedMessages(failedStream: DataFrame): StreamingQuery = {
    failedStream
      .select(
        col("source_topic").alias("Topic"),
        col("error_type").alias("Error Type"),
        substring(col("error_message"), 1, 100).alias("Error Message"),
        substring(col("original_message"), 1, 50).alias("Message Preview"),
        unix_timestamp().cast("timestamp").alias("Timestamp")
      )
      .writeStream
      .format("console")
      .option("truncate", false)
      .option("numRows", 20)
      .trigger(Trigger.ProcessingTime("1 minute")) // Update every minute
      .outputMode("append")
      .start()
  }
}