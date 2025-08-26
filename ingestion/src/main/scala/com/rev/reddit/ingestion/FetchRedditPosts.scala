package com.rev.reddit.ingestion

import play.api.libs.json.{JsValue, Json}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

object FetchRedditPosts {
  private val userAgent = RedditAuth.userAgent

  def fetchPosts(token: String, subreddit: String, limit: Int = 5): Seq[String] = {
    val backend = HttpURLConnectionBackend()

    val request = basicRequest
      .get(uri"https://oauth.reddit.com/r/$subreddit/new?limit=$limit")
      .header("Authorization", s"bearer $token")
      .header("User-Agent", userAgent)

    val response = request.send(backend)

    response.body match {
      case Right(body) =>
        val json = Json.parse(body)
        val posts = (json \ "data" \ "children").as[Seq[JsValue]]
        posts.map {post =>
          val data = post \ "data"
          Json.obj(
            "id" -> (data \ "id").as[String],
            "title" -> (data \ "title").as[String],
            "author" -> (data \ "author").as[String],
            "subreddit" -> (data \ "subreddit").as[String],
            "score" -> (data \ "score").as[Int],
            "created_utc" -> (data \ "created_utc").as[Long]
          ).toString()
        }

      case Left(error) =>
        println(s"Error fetching posts: $error")
        Seq.empty[String]
    }
  }
}