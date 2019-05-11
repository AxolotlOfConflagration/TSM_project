import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val verbose = false

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("TSM_project")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    import spark.implicits._

//    CASSANDRA TEST ------------------------------
//    val output_data = spark.range(0, 3).select($"id".as("user_id"), (rand() * 40 + 20).as("ratings"))
//    output_data.show()
//
//    DataSink.writeCassandra(output_data)
//
//    val input_data = DataLoader.readCassandra()
//    input_data.show()
//    ----------------------------------------------

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

//    DataSink.writeCsv(ratings, "ratings")
  }



  def top10PopularProduct() :DataFrame={
    implicit val session: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("TSM_project")
      .getOrCreate()
    session.sparkContext.setLogLevel("OFF")

    val recipts = DataLoader.readCsv()

    val category = DataLoader.readCsv("data/dane_kategoryzacja.csv")
    import session.sqlContext.implicits._

    recipts
      .join(category, "Produkt ID")
      .drop("Hierarchia Grupa 0 opis,Hierarchia Grupa 1 opis,Hierarchia Grupa 2 opis".split(",") : _*)
      //      .filter(_.getAs[String]("Sklep") == "Sklep13")
      .map(row => row.getAs[String]("Produkt ID") -> 1)
      .groupByKey(_._1)
      .reduceGroups((x, y) => (x._1, x._2 + y._2))
      .map(row => row._1 -> row._2._2)
      .orderBy(desc("_2"))
      .withColumnRenamed("_1" ,  "Produkt ID")
      .withColumnRenamed("_2" ,  "Ilosc")
      .join(category, "Produkt ID")
      .drop("Hierarchia Grupa 0 opis,Hierarchia Grupa 1 opis,Hierarchia Grupa 2 opis".split(",") : _*)
      .limit(10)

  }

  def getBusiestHourOfDay(): DataFrame={
    implicit val session: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("TSM_project")
      .getOrCreate()
    session.sparkContext.setLogLevel("OFF")

    val recipts = DataLoader.readCsv()//.na.drop()


    import session.sqlContext.implicits._
    recipts
      .withColumn("Godzina", split(col("Paragon godzina"), ":").getItem(0))
      .map(row => row.getAs[String]("Godzina") -> 1)
      .groupByKey(_._1)
      .reduceGroups((x, y) => (x._1, x._2 + y._2))
      .map(row => row._1 -> row._2._2)
      .orderBy(desc("_2"))
      .withColumnRenamed("_1" ,  "Godzina")
      .withColumnRenamed("_2" ,  "Ilosc")


  }
  def top3PopularCateogryProduct() : DataFrame= {
    implicit val session: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("TSM_project")
      .getOrCreate()
    session.sparkContext.setLogLevel("OFF")

    val recipts = DataLoader.readCsv()

    val category = DataLoader.readCsv("data/dane_kategoryzacja.csv")
    import session.sqlContext.implicits._
    val result = recipts
      .join(category, "Produkt ID")
      .map(row => row.getAs[String]("Produkt ID") -> 1)
      .groupByKey(_._1)
      .reduceGroups((x, y) => (x._1, x._2 + y._2))
      .map(row => row._1 -> row._2._2)
      .orderBy(desc("_2"))
      .withColumnRenamed("_1", "Produkt ID")
      .withColumnRenamed("_2", "Ilosc")
      .join(category, "Produkt ID")
      .drop("Hierarchia Grupa 1 opis")
      .drop("Hierarchia Grupa 2 opis")
      .drop("Hierarchia Grupa 3 opis")
      .drop("Produkt ID")
      .withColumnRenamed("Hierarchia Grupa 0 opis", "Kategoria")
      .orderBy(desc("Ilosc"))
      .limit(10)
    val top3 = result.select("Kategoria").distinct()
    //result.show()
    top3.limit(3)

  }
  def main(args: Array[String]): Unit = {
    top3PopularCateogryProduct().show()
  }
}
