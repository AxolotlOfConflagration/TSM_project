import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

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
    .option("location", path)
    .option("header", "true")
    .load()

  def readCassandra()(implicit ctx: SparkSession): DataFrame = ctx
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "rec", "keyspace" -> "tsm_keyspace"))
    .load
}