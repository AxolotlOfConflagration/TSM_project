import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSink {
  def writeCassandra(df: DataFrame)(): Unit = df
    .write
    .format("org.apache.spark.sql.cassandra")
    .mode("overwrite")
    .option("confirm.truncate", true)
    .options(Map( "table" -> "rec", "keyspace" -> "tsm_keyspace"))
    .save()
}
