package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, column, expr, round}
object DataFramesColumnsAndExpressions extends App{
  val spark = SparkSession.builder()
    .appName("DataFramesColumnsAndExpressions")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

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

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  carsDF.show()

  // SELECT FIRST COLUMN

  val firstColumn = carsDF.col("Acceleration")

  val carsAccelerationDF = carsDF.select(firstColumn)

  carsAccelerationDF.show()

  // VARIOUS SELECT METHODS
  import spark.implicits._

  carsDF.select(
    carsDF.col("Acceleration"),
    col("Cylinders"),
    column("Displacement"),
    'Year,
    $"Origin",
    expr("Name")
  ).show()

  // PLAIN SELECT METHOD
  carsDF.select("Cylinders", "Name").show()


  // EXPRESSIONS

  val weightInKGExpression = carsDF.col("Weight_in_lbs") / 2.2

  carsDF.select(col("Name"),
    col("Weight_in_lbs"),
    weightInKGExpression.as("Weight_IN_KG"),
    expr("Weight_in_lbs / 2.2").as("WEIGHT_IN_KG_EXPR_METHOD")
  ).show()

  // SELECT EXPRESSION METHOD

  val selectExpressionDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as WEIGHT_IN_KG"
  )
  selectExpressionDF.show()

  // COLUMN OPERATIONS

  // ADD A COLUMN
  val newCarsDF = carsDF.withColumn("WEIGHT_IN_KGS", round(col("Weight_in_lbs") / 2.2, 2))
  newCarsDF.show()

  // RENAME A COLUMN
  val carsDFRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pound")

  // CHANGE THE DATATYPE OF THE EXISTING COLUMN
  val carsDataTypeChangedDF = carsDF.withColumn("Year", col("Year").cast(DateType))

  // DROP A COLUMN
  val carsDFDroppedAcceleration = carsDF.drop("Acceleration")
  carsDFDroppedAcceleration.show()

  // FILTERING
  val nonAmericanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val nonAmericanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // CHAINING FILTERING
  val americanPowerFulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerFulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150 )
  val americanPowerFulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150 ")

  // UNIONING
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  // DISTINCT RECORDS
  val uniqueOriginDF = carsDF.select(col("Origin")).distinct()
  uniqueOriginDF.show()

}
