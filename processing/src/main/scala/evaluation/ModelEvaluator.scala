package processing.evaluation


import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ModelEvaluator {

  case class EvaluationResults(
                                accuracy: Double,
                                precision: Double,
                                recall: Double,
                                f1Score: Double,
                                confusionMatrix: Map[(Int, Int), Long],
                                classificationReport: String
                              )

  /**
   * Comprehensive model evaluation with detailed metrics
   */
  def evaluateModel(predictions: DataFrame, labelCol: String = "label",
                    predictionCol: String = "prediction"): EvaluationResults = {

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)

    // Calculate standard metrics
    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
    val recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
    val f1Score = evaluator.setMetricName("f1").evaluate(predictions)

    // Calculate confusion matrix
    val confusionMatrix = predictions
      .groupBy(labelCol, predictionCol)
      .count()
      .collect()
      .map(row => (row.getDouble(0).toInt, row.getDouble(1).toInt) -> row.getLong(2))
      .toMap

    // Generate classification report
    val classificationReport = generateClassificationReport(confusionMatrix,
      Array("Negative", "Neutral", "Positive"))

    EvaluationResults(accuracy, precision, recall, f1Score, confusionMatrix, classificationReport)
  }

  /**
   * Generate detailed classification report
   */
  def generateClassificationReport(confusionMatrix: Map[(Int, Int), Long],
                                   classNames: Array[String]): String = {
    val report = new StringBuilder
    report.append("Classification Report\n")
    report.append("=" * 50 + "\n")
    report.append(f"${"Class"}%10s ${"Precision"}%10s ${"Recall"}%10s ${"F1-Score"}%10s ${"Support"}%10s\n")
    report.append("-" * 50 + "\n")

    for (classId <- classNames.indices) {
      val className = classNames(classId)

      // Calculate metrics for each class
      val tp = confusionMatrix.getOrElse((classId, classId), 0L)
      val fp = confusionMatrix.filter { case ((actual, predicted), count) =>
        predicted == classId && actual != classId
      }.values.sum
      val fn = confusionMatrix.filter { case ((actual, predicted), count) =>
        actual == classId && predicted != classId
      }.values.sum

      val precision = if (tp + fp > 0) tp.toDouble / (tp + fp) else 0.0
      val recall = if (tp + fn > 0) tp.toDouble / (tp + fn) else 0.0
      val f1 = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0
      val support = tp + fn

      report.append(f"$className%10s ${precision}%10.3f ${recall}%10.3f ${f1}%10.3f $support%10d\n")
    }

    report.toString()
  }

  /**
   * Real-time evaluation metrics for streaming
   */
  def streamingEvaluationMetrics(predictions: DataFrame): DataFrame = {
    predictions
//      .withColumn("correct_prediction",
//        when(col("label") === col("prediction"), 1.0).otherwise(0.0))
      .groupBy(window(col("kafka_timestamp"), "5 minutes"))
      .agg(
        count("*").alias("total_predictions"),
        sum("correct_prediction").alias("correct_predictions"),
        (sum("correct_prediction") / count("*")).alias("accuracy"),
        avg("confidence").alias("avg_confidence")
      )
  }

  /**
   * A/B testing framework for comparing models
   */
  def compareModels(model1Predictions: DataFrame, model2Predictions: DataFrame,
                    testName: String): String = {
    val eval1 = evaluateModel(model1Predictions)
    val eval2 = evaluateModel(model2Predictions)

    val comparison = new StringBuilder
    comparison.append(s"A/B Test Results: $testName\n")
    comparison.append("=" * 50 + "\n")
    comparison.append(f"Metric${"Model 1"}%15s${"Model 2"}%15s${"Difference"}%15s\n")
    comparison.append("-" * 50 + "\n")
    comparison.append(f"Accuracy${eval1.accuracy}%15.4f${eval2.accuracy}%15.4f${eval2.accuracy - eval1.accuracy}%15.4f\n")
    comparison.append(f"Precision${eval1.precision}%15.4f${eval2.precision}%15.4f${eval2.precision - eval1.precision}%15.4f\n")
    comparison.append(f"Recall${eval1.recall}%15.4f${eval2.recall}%15.4f${eval2.recall - eval1.recall}%15.4f\n")
    comparison.append(f"F1-Score${eval1.f1Score}%15.4f${eval2.f1Score}%15.4f${eval2.f1Score - eval1.f1Score}%15.4f\n")

    val winner = if (eval2.f1Score > eval1.f1Score) "Model 2" else "Model 1"
    comparison.append(s"\nWinner: $winner\n")

    comparison.toString()
  }
}