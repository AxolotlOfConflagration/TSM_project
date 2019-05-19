import java.io.FileWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSink {
  val DEFAULT_PATH: String = "output_data/"
  val useCassandra = false

  def writeCassandra(df: DataFrame, table: String): Unit = if(useCassandra) {
    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode("overwrite")
      .option("confirm.truncate", true)
      .options(Map("table" -> table, "keyspace" -> "tsm_keyspace"))
      .save()
  }

  def writeCsv(df: DataFrame, fileName: String): Unit = df
    .repartition(1)
    .write
    .format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("header", "true")
    .save(DEFAULT_PATH+fileName+".csv")

  def writeCsv(data: List[(Long, Double)], names: List[String], fileName: String) = {
    val writer = new FileWriter(DEFAULT_PATH + fileName + ".csv")
    writer.append(names.mkString(",") + "\n")
    data.foreach(record => {
      writer.append(s"${record._1},${record._2}\n")
    })

    writer.close()
  }
}
