package processing.stream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import processing.config.ProcessingConfig
import processing.ml.SentimentAnalyzer
import processing.monitor.DeadLetterQueue
import processing.utils.TextProcessor

import scala.util.{Failure, Success, Try}

object KafkaStreamProcessor {

  /**
   * Read posts stream from Kafka
   */
  def readPostsStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ProcessingConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ProcessingConfig.POSTS_INPUT_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", ProcessingConfig.MAX_OFFSETS_PER_TRIGGER)
      .load()
      .withColumn("content_type", lit("post"))
  }

  /**
   * Read comments stream from Kafka
   */
  def readCommentsStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ProcessingConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ProcessingConfig.COMMENTS_INPUT_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", ProcessingConfig.MAX_OFFSETS_PER_TRIGGER)
      .load()
      .withColumn("content_type", lit("comment"))
  }

  /**
   * Parse JSON for posts
   */
  def parsePostJson = udf((jsonStr: String) => {
    Try {
      implicit val formats: DefaultFormats = DefaultFormats
      val json = parse(jsonStr)

      val id = (json \ "id").extractOpt[String].getOrElse("")
      val title = (json \ "title").extractOpt[String].getOrElse("")
      val author = (json \ "author").extractOpt[String].getOrElse("")
      val subreddit = (json \ "subreddit").extractOpt[String].getOrElse("")
      val score = (json \ "score").extractOpt[Int].getOrElse(0)
      val created_utc = (json \ "created_utc").extractOpt[Long].getOrElse(0L)

      if (id.nonEmpty && title.nonEmpty) {
        Some((id, title, author, subreddit, score, created_utc))
      } else None
    } match {
      case Success(result) => result
      case Failure(_) => None
    }
  })

  /**
   * Parse JSON for comments
   */
  def parseCommentJson = udf((jsonStr: String) => {
    Try {
      implicit val formats: DefaultFormats = DefaultFormats
      val json = parse(jsonStr)

      val id = (json \ "id").extractOpt[String].getOrElse("")
      val body = (json \ "body").extractOpt[String].getOrElse("")
      val author = (json \ "author").extractOpt[String].getOrElse("")
      val subreddit = (json \ "subreddit").extractOpt[String].getOrElse("")
      val score = (json \ "score").extractOpt[Int].getOrElse(0)
      val created_utc = (json \ "created_utc").extractOpt[Long].getOrElse(0L)
      val parent_id = (json \ "parent_id").extractOpt[String].getOrElse("")

      if (id.nonEmpty && body.nonEmpty && body != "[deleted]" && body != "[removed]") {
        Some((id, body, author, subreddit, score, created_utc, parent_id))
      } else None
    } match {
      case Success(result) => result
      case Failure(_) => None
    }
  })

  /**
   * Process unified stream (posts + comments)
   */
  def processUnifiedStream(postsStream: DataFrame, commentsStream: DataFrame,
                           sentimentModel: org.apache.spark.ml.PipelineModel,
                           spark: SparkSession): DataFrame = {
    import spark.implicits._

    // =============== POSTS PROCESSING ===============
    val postsParsed = postsStream
      .select(
        col("key").alias("kafka_key"),
        col("value").cast("string").alias("json_string"),
        col("timestamp").alias("kafka_timestamp"),
        col("content_type")
      )
      .withColumn("parsed_data", parsePostJson(col("json_string")))

    val validPosts = postsParsed.filter(col("parsed_data").isNotNull)
      .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("content_type"),
        col("parsed_data._1").alias("id"),
        col("parsed_data._2").alias("content"),
        lit("").alias("parent_id"),
        col("parsed_data._3").alias("author"),
        col("parsed_data._4").alias("subreddit"),
        col("parsed_data._5").alias("score"),
        col("parsed_data._6").alias("created_utc")
      )

    //  failed posts → DLQ
    val failedPosts = postsParsed.filter(col("parsed_data").isNull)
      .withColumn("original_message", col("json_string"))
      .withColumn("error_type", lit("JSONParseError"))
      .withColumn("error_message", lit("Invalid Post JSON"))
      .withColumn("source_topic", lit(ProcessingConfig.POSTS_INPUT_TOPIC))
    DeadLetterQueue.handleFailedMessages(failedPosts)
    DeadLetterQueue.logFailedMessages(failedPosts)

    // =============== COMMENTS PROCESSING ===============
    val commentsParsed = commentsStream
      .select(
        col("key").alias("kafka_key"),
        col("value").cast("string").alias("json_string"),
        col("timestamp").alias("kafka_timestamp"),
        col("content_type")
      )
      .withColumn("parsed_data", parseCommentJson(col("json_string")))

    val validComments = commentsParsed.filter(col("parsed_data").isNotNull)
      .select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("content_type"),
        col("parsed_data._1").alias("id"),
        col("parsed_data._2").alias("content"),
        col("parsed_data._7").alias("parent_id"),
        col("parsed_data._3").alias("author"),
        col("parsed_data._4").alias("subreddit"),
        col("parsed_data._5").alias("score"),
        col("parsed_data._6").alias("created_utc")
      )

    //  failed comments → DLQ
    val failedComments = commentsParsed.filter(col("parsed_data").isNull)
      .withColumn("original_message", col("json_string"))
      .withColumn("error_type", lit("JSONParseError"))
      .withColumn("error_message", lit("Invalid Comment JSON"))
      .withColumn("source_topic", lit(ProcessingConfig.COMMENTS_INPUT_TOPIC))
    DeadLetterQueue.handleFailedMessages(failedComments)
    DeadLetterQueue.logFailedMessages(failedComments)

    // =============== UNIFY STREAMS ===============
    val unifiedStream = validPosts.union(validComments)


    val cleanedStream = unifiedStream
      .withColumn("cleaned_text", TextProcessor.cleanText(col("content"), col("content_type")))
      .filter(col("cleaned_text") =!= "" && length(col("cleaned_text")) > ProcessingConfig.MIN_TEXT_LENGTH)


    val sentimentUDF = SentimentAnalyzer.applySentimentAnalysis(sentimentModel)
    val sentimentApplied = cleanedStream.withColumn("sentiment_result", sentimentUDF(col("cleaned_text")))

    val goodSentiment = sentimentApplied.filter(col("sentiment_result").isNotNull)
      .select(
        col("*"),
        col("sentiment_result._1").alias("sentiment"),
        col("sentiment_result._2").alias("sentiment_score"),
        col("sentiment_result._3").alias("confidence")
      )
      .drop("sentiment_result")

    // failed sentiment → DLQ
    val failedSentiment = sentimentApplied.filter(col("sentiment_result").isNull)
      .withColumn("original_message", col("cleaned_text"))
      .withColumn("error_type", lit("SentimentAnalysisError"))
      .withColumn("error_message", lit("Null sentiment result"))
      .withColumn("source_topic", col("content_type"))
    DeadLetterQueue.handleFailedMessages(failedSentiment)
    DeadLetterQueue.logFailedMessages(failedSentiment)


    val sentimentStream = goodSentiment
      .withColumn("processing_timestamp", current_timestamp())
      .withColumn("model_version", lit(ProcessingConfig.MODEL_VERSION))
      .withColumn("text_length", length(col("cleaned_text")))
      .withColumn("engagement_score", col("score"))
      .filter(col("confidence") >= ProcessingConfig.MIN_CONFIDENCE_THRESHOLD)

    //streaming metrics for live eval
//    val metricsStream = ModelEvaluator.streamingEvaluationMetrics(sentimentStream)
//    metricsStream.writeStream
//      .outputMode("update")
//      .format("console")
//      .option("truncate", false)
//      .trigger(Trigger.ProcessingTime("1 minute"))
//      .start()

    sentimentStream
  }

  /**
   * Write processed results to Kafka for Elasticsearch consumption
   */
  def writeToKafka(processedStream: DataFrame): StreamingQuery = {
    val kafkaOutput = processedStream
      .select(
        col("id").alias("key"),
        to_json(struct(
          col("id"),
          col("content").alias("original_content"),
          col("cleaned_text"),
          col("content_type"),
          col("author"),
          col("subreddit"),
          col("score"),
          col("created_utc"),
          col("parent_id"),
          col("sentiment"),
          col("sentiment_score"),
          col("confidence"),
          col("text_length"),
          col("engagement_score"),
          col("processing_timestamp"),
          col("model_version")
        )).alias("value")
      )

    kafkaOutput.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ProcessingConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("topic", ProcessingConfig.OUTPUT_TOPIC)
      .option("checkpointLocation", s"${ProcessingConfig.CHECKPOINT_LOCATION}/kafka-output")
      .trigger(Trigger.ProcessingTime(ProcessingConfig.PROCESSING_INTERVAL))
      .outputMode("append")
      .start()
  }

  /**
   * Console output for monitoring and debugging
   */
  def writeToConsole(processedStream: DataFrame): StreamingQuery = {
    processedStream
      .select(
        col("id"),
        col("content_type"),
        substring(col("content"), 1, 60).alias("content_preview"),
        col("subreddit"),
        col("author"),
        col("sentiment"),
        round(col("sentiment_score"), 3).alias("score"),
        round(col("confidence"), 3).alias("conf"),
        col("engagement_score").alias("engagement")
      )
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .option("numRows", 20)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
  }
}
