import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

case class DataLoader (path: String = "/home/sleter/Documents/Github/TSM_project/data/dane_paragony.xlsx")
{
  def readXslx(ctx: SparkSession): DataFrame = ctx
    .read
    .format("com.crealytics.spark.excel")
    .option("location", path)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "False")
    .load()

  def readCsv(ctx: SparkSession): DataFrame = ctx
    .read
    .format("csv")
    .option("location", path)
    .option("header", "true")
    .load()
}
