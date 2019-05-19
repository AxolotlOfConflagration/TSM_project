import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.fpm.FPGrowth
import scala.collection.mutable

object Main {
  val verbose = false

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("TSM_project")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    val data = DataLoader.readXslx()
    val (shopRecs, itemRecs) = als(data)
    saveRecommendation(shopRecs, itemRecs)

    println("Top 10 Popular Product")
    top10PopularProduct().show()
    println("Busiest Hour of Day")
    getBusiestHourOfDay().show(10)
    println("Top 3 Popular Categpry Product")
    top3PopularCategoryProduct().show()

    val (mostFreqItemInABasket, assoRules) = marketBasketAnalysis()
    println("Most Freq Item In a Basket")
    mostFreqItemInABasket.show()
    println("Association Rules")
    assoRules.show()

    DataSink.writeCassandra(top10PopularProduct(), "top10products")
    DataSink.writeCsv(top10PopularProduct(), "top10products")
    DataSink.writeCassandra(getBusiestHourOfDay(), "busiesthourofday")
    DataSink.writeCsv(getBusiestHourOfDay(), "busiesthourofday")
    DataSink.writeCassandra(top3PopularCategoryProduct(), "top3popularcategoryproduct")
    DataSink.writeCsv(top3PopularCategoryProduct(), "top3popularcategoryproduct")
    DataSink.writeCassandra(mostFreqItemInABasket, "mostpopulariteminabasket")
    DataSink.writeCsv({
      mostFreqItemInABasket
        .withColumn("items", concat_ws(",", col("items")))
    }, "mostpopulariteminabasket")

    DataSink.writeCassandra(assoRules, "ifthen")
    DataSink.writeCsv({
      assoRules
        .withColumn("antecedent", concat_ws(",", col("antecedent")))
        .withColumn("consequent", concat_ws(",", col("consequent")))
    }, "ifthen")
  }

  /**
    * Recommendation using ALS algorithm
    *
    * @param data Receipt data that is used for recommendation
    * @return Tuple (recommendation for shops, recommendation for items)
    */
  def als(data: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._

    val storeItemCount = data
      .groupBy($"Sklep", $"Produkt ID")
      .count()
      .na.drop()
      .orderBy(desc("count"))

    if (verbose) storeItemCount.show()

    val storeTotalItemsSold =
      storeItemCount
        .groupBy($"Sklep")
        .sum("count")

    if (verbose) storeTotalItemsSold.show()

    val ratings = storeItemCount
      .join(storeTotalItemsSold, "Sklep")
      .select(
        regexp_extract($"Sklep", """(\d+)""", 1) cast "int" as "Sklep",
        $"Produkt ID",
        $"count" / $"sum(count)" as "rating")
      .orderBy(desc("rating"))

    if (verbose) ratings.show()

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

    val shopRecs = model
      .recommendForAllUsers(10)
      .withColumnRenamed("Sklep", "shop_id")
      .withColumn("products", to_json(col("recommendations")))
      .drop(col("recommendations"))

    val itemRecs = model
      .recommendForAllItems(10)
      .withColumnRenamed("Produkt ID", "product_id")
      .withColumn("shops", to_json(col("recommendations")))
      .drop(col("recommendations"))

    (shopRecs, itemRecs)
  }

  def saveRecommendation(shopRecs: DataFrame, itemRecs: DataFrame)(implicit spark: SparkSession): Unit = {
    println("We recommend for shops to stock up on these items:")
    shopRecs.show()
    println("We recommend for warehouses to send items for those shops:")
    itemRecs.show()

    DataSink.writeCassandra(shopRecs, "shoprecs")
    DataSink.writeCassandra(itemRecs, "itemrecs")
  }

  def marketBasketAnalysis()(implicit session: SparkSession): (DataFrame, DataFrame) = {
    import session.sqlContext.implicits._
    val receipts = DataLoader.readCsv()(session)
    val category = DataLoader.readCsv("data/dane_kategoryzacja.csv")(session)

    val removeDuplicates: mutable.WrappedArray[String] => mutable.WrappedArray[String] = _.distinct
    val uniqueProduct = udf(removeDuplicates)

    val basketItems = receipts
      .join(category, "Produkt ID")
      .drop("""Sklep, Paragon godzina, Promocja A, Promocja B,
      Wartość netto sprzedaży z paragonu, Rok i miesiac, 
      Hierarchia Grupa 0 opis, Hierarchia Grupa 1 opis, Hierarchia Grupa 2 opis""".split(", "): _*)
      .groupBy("Paragon numer")
      .agg(collect_list($"Hierarchia Grupa 3 opis"))
      .withColumn("collect_list(Hierarchia Grupa 3 opis)", uniqueProduct($"collect_list(Hierarchia Grupa 3 opis)"))
      .withColumnRenamed("collect_list(Hierarchia Grupa 3 opis)", "Items")

    val fpgrowth = new FPGrowth().setItemsCol("Items").setMinSupport(0.001).setMinConfidence(0)
    val model = fpgrowth.fit(basketItems)

    val mostFreqItemInABasket = model.freqItemsets
      .orderBy(desc("freq"))
      .withColumn("id", monotonically_increasing_id)


    val assoRules = model.associationRules
      .orderBy(desc("confidence"))
      .withColumn("id", monotonically_increasing_id)

    (mostFreqItemInABasket, assoRules)

  }

  def top10PopularProduct()(implicit session: SparkSession): DataFrame = {

    val receipts = DataLoader.readCsv()(session)

    val category = DataLoader.readCsv("data/dane_kategoryzacja.csv")(session)

    import session.sqlContext.implicits._

    receipts
      .join(category, "Produkt ID")
      .drop("Hierarchia Grupa 0 opis,Hierarchia Grupa 1 opis,Hierarchia Grupa 2 opis".split(","): _*)
      //      .filter(_.getAs[String]("Sklep") == "Sklep13")
      .map(row => row.getAs[String]("Produkt ID") -> 1)
      .groupByKey(_._1)
      .reduceGroups((x, y) => (x._1, x._2 + y._2))
      .map(row => row._1 -> row._2._2)
      .orderBy(desc("_2"))
      .withColumnRenamed("_1", "Produkt ID")
      .withColumnRenamed("_2", "Ilosc")
      .join(category, "Produkt ID")
      .drop("Hierarchia Grupa 0 opis,Hierarchia Grupa 1 opis,Hierarchia Grupa 2 opis".split(","): _*)
      .limit(10)
      .withColumnRenamed("Produkt ID", "product_id")
      .withColumnRenamed("Ilosc", "amount")
      .withColumnRenamed("Hierarchia Grupa 3 opis", "description")

  }

  def getBusiestHourOfDay()(implicit session: SparkSession): DataFrame = {

    val receipts = DataLoader.readCsv()(session) //.na.drop()

    import session.sqlContext.implicits._

    receipts
      .withColumn("Godzina", split(col("Paragon godzina"), ":").getItem(0))
      .map(row => row.getAs[String]("Godzina") -> 1)
      .groupByKey(_._1)
      .reduceGroups((x, y) => (x._1, x._2 + y._2))
      .map(row => row._1 -> row._2._2)
      .orderBy(desc("_2"))
      .withColumnRenamed("_1", "hour")
      .withColumnRenamed("_2", "amount")
  }

  def top3PopularCategoryProduct()(implicit session: SparkSession): DataFrame = {

    val receipts = DataLoader.readCsv()(session)

    val category = DataLoader.readCsv("data/dane_kategoryzacja.csv")(session)

    import session.sqlContext.implicits._

    val result = receipts
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
    top3.limit(3).withColumnRenamed("Kategoria", "category")

  }
}
