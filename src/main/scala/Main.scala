import org.apache.spark.sql.{SQLContext, SparkSession}

object Main{
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .master("local")
          .appName("TSM_project")
          .getOrCreate()

        val dataLoader = new DataLoader()
        var data = dataLoader.readXslx(spark)

        data.printSchema()
        data.show()
    }

}
