import com.johnsnowlabs.nlp.DocumentAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DataCleaning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Data Cleaning")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

    //    reading from kafka
        val rawStream = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers","localhost:9092")
          .option("subscribe","reddit-posts")
          .load()

        val rawDF = rawStream.selectExpr("CAST(value AS STRING)").as[String]
        val sampleData = Seq(
          "I loved this movie",
          "i hated it completely",
          "iron man sucks",
          "phantom of the opera is a masterpiece"
        ).toDF("text")


    //    preprocessing text
        val df = rawDF.toDF("text")

    //    pipeline for nlp processing
        val documentAssembler = new DocumentAssembler()
          .setInputCol("text")
          .setOutputCol("document")

//        val tokenizer = new Tokenizer()
//          .setInputCols("document")
//          .setOutputCol("token")
//
//        val normalizer = new Normalizer()
//          .setInputCols("token")
//          .setOutputCol("normalized")
//
//        val sentimentModel = SentimentDLModel.pretrained("sentimentdl_use_twitter","en")
//          .setInputCols("document","normalized")
//          .setOutputCol("sentiment")
//
//        val pipeline = new Pipeline().setStages(Array(
//          documentAssembler,
//          tokenizer,
//          normalizer,
//          sentimentModel
//        ))
//
//        val processedDF = pipeline.fit(df).transform(df)
//        val model = pipeline.fit(sampleData)
//        val result = model.transform(sampleData)
//        val predictions = model.transform(sampleData)
//        val scored = predictions.select($"text", $"label", $"sentiment.result".getItem(0).as("predicted"))
//
//        result.select("text","sentiment.result").show(false)
//        scored.show()
    println("testing")
  }

}
