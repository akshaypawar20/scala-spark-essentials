package dataframe_basics
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object DataFrames extends App{

  // CREATE A SPARK SESSION
  val spark = SparkSession.builder()
    .appName("Dataframe Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // SET THE LOG LEVEL TO ERROR
  spark.sparkContext.setLogLevel("Error")

  // READ A FILE WITH INFERSCHEMA
  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  carsDF.show()

  // READ A FILE WITHOUT INFERSCHEMA

  val enforcedSchema = StructType(Array(
    StructField("Acceleration", DoubleType, true),
    StructField("Cylinders", IntegerType, true),
    StructField("Displacement", DoubleType, true),
    StructField("Horsepower", IntegerType, true),
    StructField("Miles_per_Gallon", DoubleType, true),
    StructField("Name", StringType, true),
    StructField("Origin", StringType, true),
    StructField("Weight_in_lbs", IntegerType, true),
    StructField("Year", DateType, true)
  )
  )

  var carsDFEnforcedSchema = spark.read
    .format("json")
    .schema(enforcedSchema)
    .load("src/main/resources/data/cars.json")

  carsDFEnforcedSchema.printSchema()
  carsDFEnforcedSchema.show()


  // CREATE A MANUAL DATAFRAME
  val data = Seq(
    (12.0, 8, 307.0, 130, 18.0, "chevrolet","USA",3504,"1970-01-01"),
    (13.0, 8, 307.0, 130, 18.0, "skylark","USA",3504,"1970-01-01"),
    (14.0, 8, 307.0, 130, 18.0, "rebel","USA",3504,"1970-01-01"),
    (15.0, 8, 307.0, 130, 18.0, "ford","USA",3504,"1970-01-01"),
    (16.0, 8, 307.0, 130, 18.0, "impala","USA",3504,"1970-01-01")
  )
  val manualCarsDFSchema = Seq("Acceleration","Cylinders", "Displacement", "Horsepower", "Miles_per_Gallon", "Name", "Origin", "Weight_in_lbs", "Year")
  print(manualCarsDFSchema)


  val manualCarsDF = spark.createDataFrame(data).toDF("Acceleration","Cylinders", "Displacement", "Horsepower", "Miles_per_Gallon", "Name", "Origin", "Weight_in_lbs", "Year")
  manualCarsDF.printSchema()
  manualCarsDF.show()

  // TYPE CASTING

  var carsAnotherDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  carsAnotherDF = carsAnotherDF.withColumn("Year", col("Year").cast(DateType))
  carsAnotherDF.printSchema()

  // Another Way of Reading File using Map

  val moviesDF = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true"
    ))
    .load("src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.printSchema()

  // WRITE A DATAFRAME

  moviesDF.select("Production_Budget").write
    .format("json")
    .mode("Overwrite")
    .save("src/main/resources/output/")
}
