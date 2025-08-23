lazy val root = (project in file("."))
  .aggregate(ingestion, processing, storage, visualization, utils)
  .settings(
    name := "RedditSentimentPipeline",
    scalaVersion := "2.12.18"
  )

lazy val utils = (project in file("utils"))
  .settings(
    name := "reddit-utils",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "2.0.9",
      "ch.qos.logback" % "logback-classic" % "1.4.11"
    )
  )

lazy val ingestion = (project in file("ingestion"))
  .dependsOn(utils)
  .settings(
    name := "reddit-ingestion",
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
      "org.apache.kafka" %% "kafka" % "3.6.0"
    )
  )

lazy val processing = (project in file("processing"))
  .dependsOn(utils)
  .settings(
    name := "reddit-processing",
    libraryDependencies ++= Seq(
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.3.0",
      "org.apache.spark" %% "spark-core" % "3.5.0" % "compile" exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "compile" exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
      "org.apache.spark" %% "spark-mllib" % "3.5.0" % "compile" exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
    )
  )

lazy val storage = (project in file("storage"))
  .dependsOn(utils)
  .settings(
    name := "reddit-storage",
    libraryDependencies ++= Seq(
    )
  )

lazy val visualization = (project in file("visualization"))
  .dependsOn(utils)
  .settings(
    name := "reddit-visualization",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % "2.8.5",
      "com.typesafe.akka" %% "akka-stream" % "2.8.5",
      "com.typesafe.akka" %% "akka-http" % "10.5.2",
      "de.heikoseeberger" %% "akka-http-circe" % "1.39.2",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6"
    )
  )
