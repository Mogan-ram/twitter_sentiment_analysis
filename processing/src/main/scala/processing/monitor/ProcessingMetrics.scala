package processing.monitor


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ProcessingMetrics {

  /**
   * Calculate real-time processing metrics
   */
  def calculateProcessingMetrics(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    df.groupBy(
      window(col("processing_timestamp"), "5 minutes"),
      col("content_type"),
      col("subreddit")
    ).agg(
      count("*").alias("total_processed"),
      avg("sentiment_score").alias("avg_sentiment"),
      avg("confidence").alias("avg_confidence"),
      sum(when(col("sentiment") === "positive", 1).otherwise(0)).alias("positive_count"),
      sum(when(col("sentiment") === "negative", 1).otherwise(0)).alias("negative_count"),
      sum(when(col("sentiment") === "neutral", 1).otherwise(0)).alias("neutral_count"),
      max("engagement_score").alias("max_engagement"),
      avg("text_length").alias("avg_text_length"),
      countDistinct("author").alias("unique_authors")
    )
  }

  /**
   * Get top engaging content by type
   */
  def getTopEngagingContent(df: DataFrame, contentType: String, limit: Int = 10): DataFrame = {
    df.filter(col("content_type") === contentType)
      .orderBy(desc("engagement_score"))
      .select("id", "content", "subreddit", "sentiment", "engagement_score", "author")
      .limit(limit)
  }

  /**
   * Calculate sentiment distribution by subreddit
   */
  def getSentimentBySubreddit(df: DataFrame): DataFrame = {
    df.groupBy("subreddit", "content_type")
      .agg(
        count("*").alias("total_count"),
        avg("sentiment_score").alias("avg_sentiment_score"),
        sum(when(col("sentiment") === "positive", 1).otherwise(0)).alias("positive"),
        sum(when(col("sentiment") === "negative", 1).otherwise(0)).alias("negative"),
        sum(when(col("sentiment") === "neutral", 1).otherwise(0)).alias("neutral")
      )
      .withColumn("positive_ratio", col("positive") / col("total_count"))
      .withColumn("negative_ratio", col("negative") / col("total_count"))
      .withColumn("neutral_ratio", col("neutral") / col("total_count"))
  }
}
