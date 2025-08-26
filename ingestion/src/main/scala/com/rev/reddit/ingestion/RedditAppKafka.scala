package com.rev.reddit.ingestion

import play.api.libs.json.Json

object RedditAppKafka {
  def main(args: Array[String]): Unit = {
    val postTopic = "reddit-posts"
    val commentTopic = "reddit-comments"
    val subreddit = "technology"

    val producer = KafkaProducerUtil.createProducer()
    val token = RedditAuth.getAccessToken

    println(s"Streaming r/$subreddit -> Kafka [$postTopic, $commentTopic]")

    var seenPosts = Set[String]()
    var seenComments = Set[String]()

    while (true) {
      val posts = FetchRedditPosts.fetchPosts(token, subreddit, 5)
      posts.foreach { postJson =>
        val postId = (Json.parse(postJson) \ "id").as[String]
        if (!seenPosts.contains(postId)) {
          KafkaProducerUtil.senToKafka(producer, postTopic, postJson)
          println(s"Sent Post: $postJson")
          seenPosts += postId
        }
      }

      val comments = FetchRedditComments.fetchComments(token, subreddit, 5)
      comments.foreach { commentJson =>
        val commentId = (Json.parse(commentJson) \ "id").as[String]
        if (!seenComments.contains(commentId)) {
          KafkaProducerUtil.senToKafka(producer, commentTopic, commentJson)
          println(s"Sent Comment: $commentJson")
          seenComments += commentId
        }
      }

      producer.flush()
      Thread.sleep(5000)
    }
  }
}
