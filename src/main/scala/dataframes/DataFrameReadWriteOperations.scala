package dataframes
import org.apache.spark.sql.functions.{col, date_format, to_date}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object DataFrameReadWriteOperations extends App{

  // CREATE A SPARK SESSION
  val spark = SparkSession.builder()
    .appName("Dataframe Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // SET THE LOG LEVEL TO ERROR
  spark.sparkContext.setLogLevel("Error")

  // READ A DATAFRAME USING INFER SCHEMA

  val inferredCarsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  inferredCarsDF.show()

  // READ A DATAFRAME USING MANUAL SCHEMA

  val carsSchema = StructType(Array(
    StructField("Acceleration", DoubleType, true),
    StructField("Cylinders", IntegerType, true),
    StructField("Displacement", DoubleType, true),
    StructField("Horsepower", IntegerType, true),
    StructField("Miles_per_Gallon", DoubleType, true),
    StructField("Name", StringType, true),
    StructField("Origin", StringType, true),
    StructField("Weight_in_lbs", IntegerType, true),
    StructField("Year", DateType, true)
  ))

  val carsDFManualSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  carsDFManualSchema.show()
  carsDFManualSchema.printSchema()

  // MANUAL DATAFRAME CREATION FROM TUPLE

  val manualData = Seq(
    ("Akshay", "Pawar", "Pune", 26, "2020-10-22"),
    ("Ashwen", "Venkatesh", "Chennai", 26, "2021-10-22"),
    ("Nishant", "Kumar", "Pune", 28, "2021-10-22"),
    ("Rutik", "Lohade", "Pune", 24, "2022-10-22")

  )

  val manualDF = spark.createDataFrame(manualData)

  // DIFFERENT READ MODES

  val moviesDF = spark.read
    .format("json")
    .option("mode", "FAILFAST") // PERMISSIVE, FAILFAST, DROPMALFORMED
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // OPTION ALTERNATIVE

  val moviesDFAlternative = spark.read
    .format("json")
    .options(Map(
      "inferSchema" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "src/main/resources/data/movies.json"
    ))
    .load()

  // DIFFERENT DATA SOURCES

  // CSV FILE
  val stocksDF = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "sep" -> ",",
      "inferSchema" -> "true",
      "path" -> "src/main/resources/data/stocks.csv"
    ))
    .load()

  // PARQUET FILE
  val yellowTaxiDF = spark.read
    .format("parquet")
    .load("src/main/resources/data/yellow_taxi_jan_25_2018")

  yellowTaxiDF.show()

  // TEXT FILE

  val sampleTextDF = spark.read
    .format("text")
    .load("src/main/resources/data/sample_text.txt")

  sampleTextDF.show()



  // DATAFRAME WRITE OPERATION
  moviesDF.write
    .format("json")
    .mode(SaveMode.Overwrite) // Overwrite, Append, ErrorIfExists, Ignore
    .save("src/main/resources/output/movies")

}
