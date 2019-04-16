import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val verbose = true

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("TSM_project")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    val data = DataLoader.readXslx()

    val storeItemCount = data
      .groupBy($"Sklep", $"Produkt ID")
      .count()
      .na.drop()
      .orderBy(desc("count"))

    if(verbose) storeItemCount.show()

    val storeTotalItemsSold =
      storeItemCount
        .groupBy($"Sklep")
        .sum("count")

    if(verbose) storeTotalItemsSold.show()

    val ratings = storeItemCount
      .join(storeTotalItemsSold, "Sklep")
      .select(
        regexp_extract($"Sklep", """(\d+)""", 1) cast "int" as "Sklep",
        $"Produkt ID",
        $"count" / $"sum(count)" as "rating")
      .orderBy(desc("rating"))

    if(verbose) ratings.show()

    val Array(traning, test) = ratings.randomSplit(Array(0.85, 0.15), 42)

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("Sklep")
      .setItemCol("Produkt ID")
      .setRatingCol("rating")
    val model = als.fit(traning)
    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    val shopRecs = model.recommendForAllUsers(10)
    val itemRecs = model.recommendForAllItems(10)

    println("We recommend for shops to stock up on these items:")
    shopRecs.show()
    println("We recommend for warehouses to send items for those shops:")
    itemRecs.show()
  }
}
