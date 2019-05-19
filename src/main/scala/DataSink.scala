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
}
