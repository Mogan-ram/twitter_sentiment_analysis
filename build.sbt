lazy val root = (project in file("."))
  .aggregate(ingestion, processing, storage, visualization)
  .settings(
    name := "RedditSentimentPipeline",
    scalaVersion := "2.12.18" // or match Spark version
  )

lazy val ingestion = (project in file("ingestion"))
  .settings(
    name := "reddit-ingestion",
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play-json" % "2.9.4",
      "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
      "org.apache.kafka" %% "kafka" % "3.6.0"
    )
  )

lazy val processing = (project in file("processing"))
  .settings(
    name := "reddit-processing",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.5.0" % "provided",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.3.0"
    )
  )

lazy val storage = (project in file("storage"))
  .settings(
    name := "reddit-storage",
    libraryDependencies ++= Seq(
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0"
    )
  )

lazy val visualization = (project in file("visualization"))
  .settings(
    name := "reddit-visualization",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % "2.8.5",
      "com.typesafe.akka" %% "akka-stream" % "2.8.5",
      "com.typesafe.akka" %% "akka-http" % "10.5.2",
      "de.heikoseeberger" %% "akka-http-circe" % "1.39.2", // for JSON (optional, Circe)
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6"
    )
  )

