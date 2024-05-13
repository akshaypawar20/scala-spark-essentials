package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, current_date, current_timestamp, datediff, day, initcap, lit, lower, month, regexp_replace, to_date, upper, year}

object SparkDataTypesAndNulls extends App{

  val spark = SparkSession.builder()
    .appName("SparkDataTypes")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // PLAIN TEXT
  val dummyDF = spark.range(10).toDF("Values")
    .withColumn("LIT_CONSTANT", lit("CONSTANT_VALUES"))

  // BOOLEAN
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodMovieFilter = col("IMDB_Rating") > 8.0
  val goodDramaMovieFilter = dramaFilter and goodMovieFilter

  val goodDramaMovieDF = moviesDF.select(col("Title"), goodDramaMovieFilter.as("GoodDramaMovie"))
  goodDramaMovieDF.filter(col("GoodDramaMovie"))

  // Numbers
  val moviesAvgRatingDF = moviesDF.select((col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating") / 2).as("AVG_RATING"))
  moviesAvgRatingDF.na.drop()

  // STRING

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // INITCAP, LOWER, UPPER
  carsDF.select(initcap(col("Name")), lower(col("Name")), upper(col("Name")))

  // CONCAT, CONCAT_WS
  carsDF.select(concat(col("Name"), col("Origin")).as("Concat"))
  carsDF.select(concat_ws(" ", col("Name"),col("Origin")).as("Concat with space"))

  // CONTAINS
  carsDF.select(col("Name"), (col("Name") contains "chevrolet").as("Chevrolet"))
    .filter("Chevrolet").drop("Chevrolet")

  // DATES

  moviesDF.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release_Date"))
    .withColumn("Year", year(col("Actual_Release_Date")))
    .withColumn("Month", month(col("Actual_Release_Date")))
    .withColumn("Day", month(col("Actual_Release_Date")))
    .withColumn("Today", current_date())
    .withColumn("Now", current_timestamp())
    .withColumn("MOVIE AGE", datediff(col("Today"), col("Actual_Release_Date")) / 365)


  // DEALING WITH NULLS
  moviesDF.show(5)

  // FILLING THE NAs
  val moviesDFFiltered = moviesDF.na.fill(Map(
    "Director" -> "Unknown",
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10
  ))
  moviesDFFiltered.show(5)

  // DROPPING THE NAs
  val droppedNAs = moviesDF.na.drop()

  droppedNAs.show(5)

}
