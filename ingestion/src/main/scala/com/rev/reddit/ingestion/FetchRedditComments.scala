package com.rev.reddit.ingestion

import play.api.libs.json.{JsValue, Json}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

object FetchRedditComments {
  private val userAgent = RedditAuth.userAgent

  def fetchComments(token: String, subreddit: String, limit: Int = 5): Seq[String] = {
    val backend = HttpURLConnectionBackend()

    val request = basicRequest
      .get(uri"https://oauth.reddit.com/r/$subreddit/comments?limit=$limit")
      .header("Authorization", s"bearer $token")
      .header("User-Agent", userAgent)

    val response = request.send(backend)

    response.body match {
      case Right(body) =>
        val json = Json.parse(body)
        val comments = (json \ "data" \ "children").as[Seq[JsValue]]
        comments.map { comment =>
          val data = comment \ "data"

          import play.api.libs.json.Json._

          Json.obj (
            "id" -> (data \ "id").as[String],
            "body" -> ((data \ "body").asOpt[String].getOrElse(""): String),
            "author" -> ((data \ "author").asOpt[String].getOrElse(""): String),
            "subreddit" -> (data \ "subreddit").as[String],
            "score" -> (data \ "score").as[Int],
            "created_utc" -> (data \ "created_utc").as[Long],
            "parent_id" -> (data \ "parent_id").as[String]
          ).toString()
        }

      case Left(error) =>
        println(s"Error fetching comments: $error")
        Seq.empty[String]
    }
  }
}