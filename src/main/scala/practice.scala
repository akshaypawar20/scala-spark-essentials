import org.apache.spark.sql.{SparkSession}

object practice extends App {

  val spark = SparkSession.builder()
    .appName("Practice")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("Error")

  val DF = spark.range(10).toDF("Values")
  DF.show()

}
