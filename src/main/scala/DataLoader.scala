import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {
  val DEFAULT_PATH: String = "data/dane_paragony."

  def readXslx(path: String = DEFAULT_PATH+"xlsx")(implicit ctx: SparkSession): DataFrame = ctx
    .read
    .format("com.crealytics.spark.excel")
    .option("location", path)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "False")
    .load()

  def readCsv(path: String = DEFAULT_PATH+"csv")(implicit ctx: SparkSession): DataFrame = ctx
    .read
    .format("csv")
    .option("header", "true")
    .load(path)
}