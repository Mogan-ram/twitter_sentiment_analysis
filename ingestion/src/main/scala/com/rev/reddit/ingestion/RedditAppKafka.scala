package com.rev.reddit.ingestion

import org.apache.kafka.clients.producer.KafkaProducer
import play.api.libs.json.Json
import sttp.client3._

import java.util.Properties

object RedditFetchData {
  //reddit-credentials
  val clientId = "RSetxUYaFfIir26DCPHV0A"
  val clientSecret = "j1Gxd3__tIoBMWKnoeVicsZG3aN9EQ"
  val userAgent = "sentiment-analysis:0.1 (by /u/Divya_Chilagani)"

  //kafka configuration
  def createProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serializable.StringSerializer")
    props.put("acks", "all")
    new KafkaProducer[String, String](props)
  }

  //get OAuth token
  def getAccessToken(): String = {
    val backend = HttpURLConnectionBackend()

    val request = basicRequest
      .post(uri"https://www.reddit.com/api/v1/access_token")
      .auth.basic(clientId, clientSecret)
      .body(Map("grant_type" -> "client_credentials"))
      .header("User_Agent", userAgent)

    val response = request.send(backend)

    response.body match {
      case Right(body) =>
        val json = Json.parse(body)
        (json \ "access_token").as[String]
      case Left(error) =>
        throw new RuntimeException(s"Failed to get token: $error")
    }
  }

  //fetch posts
  def fetchPosts(token: String, subreddit: String, limit: Int = 5): Seq[String] = {
    val backend = HttpURLConnectionBackend()

    val request = basicRequest
      .get(uri"https://oauth.reddit.com/r/$subreddit/new?limit=$limit")
      .header("Authorization", s"bearer $token")
      .header("User-Agent", userAgent)

    val response = request.send(backend)
    
  }
}
