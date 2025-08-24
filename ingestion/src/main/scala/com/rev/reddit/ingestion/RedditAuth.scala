package com.rev.reddit.ingestion

import play.api.libs.json.Json
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

object RedditAuth {
  val clientId = "RSetxUYaFfIir26DCPHV0A"
  val clientSecret = "j1Gxd3__tIoBMWKnoeVicsZG3aN9EQ"
  val userAgent = "sentiment-analysis:0.1 (by /u/Divya_Chilagani)"

  def getAccessToken: String = {
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
//        add a log for data fetched successfully
      case Left(error) =>
//        log that fetching data is failed along with the error message
        throw new RuntimeException(s"Failed to get token: $error")
    }
  }
}