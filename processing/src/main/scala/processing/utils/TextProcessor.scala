package processing.utils

import org.apache.spark.sql.functions._

object TextProcessor {

  /**
   * Enhanced text cleaning for Reddit content (posts and comments)
   */
  val cleanText = udf((text: String, contentType: String) => {
    if (text == null || text.trim.isEmpty) {
      ""
    } else {
      text
        .toLowerCase()
        // Remove Reddit-specific content
        .replaceAll("""\[deleted\]""", " ")
        .replaceAll("""\[removed\]""", " ")
        .replaceAll("""\[EDIT:\s*.*?\]""", " ")
        // Remove URLs
        .replaceAll("""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+""", " ")
        // Remove Reddit mentions
        .replaceAll("""r/\w+""", " ")
        .replaceAll("""u/\w+""", " ")
        .replaceAll("""/u/\w+""", " ")
        .replaceAll("""/r/\w+""", " ")
        // Remove special characters but keep basic punctuation for sentiment
        .replaceAll("""[^\w\s.,!?'-]""", " ")
        // Handle common Reddit abbreviations
        .replaceAll("""\btl;dr\b""", "summary")
        .replaceAll("""\bimo\b""", "in my opinion")
        .replaceAll("""\bimho\b""", "in my opinion")
        // Replace multiple spaces/newlines with single space
        .replaceAll("""\s+""", " ")
        .trim
    }
  })

  /**
   * Extract meaningful content based on type
   */
  val extractContent = udf((title: String, body: String, contentType: String) => {
    contentType match {
      case "post" =>
        if (title != null && title.trim.nonEmpty) title.trim else ""
      case "comment" =>
        if (body != null && body.trim.nonEmpty &&
          body.trim != "[deleted]" && body.trim != "[removed]") body.trim else ""
      case _ => ""
    }
  })
}
