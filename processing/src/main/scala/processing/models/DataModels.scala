package processing.models

case class RedditPost(
                       id: String,
                       title: String,
                       author: String,
                       subreddit: String,
                       score: Int,
                       created_utc: Long,
                       content_type: String = "post"
                     )

case class RedditComment(
                          id: String,
                          body: String,
                          author: String,
                          subreddit: String,
                          score: Int,
                          created_utc: Long,
                          parent_id: String,
                          content_type: String = "comment"
                        )

case class ProcessedContent(
                             id: String,
                             content: String,
                             cleaned_text: String,
                             author: String,
                             subreddit: String,
                             score: Int,
                             created_utc: Long,
                             content_type: String,
                             sentiment: String,
                             sentiment_score: Double,
                             confidence: Double,
                             text_length: Int,
                             engagement_score: Int,
                             processing_timestamp: Long,
                             model_version: String
                           )