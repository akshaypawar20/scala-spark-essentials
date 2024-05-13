package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, avg, min, max, approx_count_distinct, col, count, countDistinct}

object DataFramesAggregations extends App{

  val spark = SparkSession.builder()
    .appName("DataFramesAggregations")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // COUNTING THE VALUES IN A COLUMN
  val countMajorGenreDF = moviesDF.select(count(col("Major_Genre")).as("Total_Genre_Count"))
  moviesDF.selectExpr("count(Major_Genre) as Total_Genre_Count")

  // COUNT ALL THE VALUES IN A DATAFRAME
  val countAllValuesDF = moviesDF.select(count(col("*")).as("Total_Count"))
  moviesDF.selectExpr("count(*) as Total_Count")

  // COUNT DISTINCT COUNT
  val distinctCountDF2 = moviesDF.select(countDistinct(col("Director")))

  // APPROX DISTINCT COUNT
  val approxDistinctCountDF = moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // MIN MAX
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("max(IMDB_Rating) as MAX_IMDB_RATING")

  // SUM AVG

  val sumUSGrossDF = moviesDF.select(sum(col("US_Gross")))

  // GROUPING
  val moviesDFWithoutNull = moviesDF.na.drop()

  val averageIMDBDF= moviesDFWithoutNull.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Average_IMDB_Rating"),
      sum("US_Gross").as("Sum of Gross Revenue")
    )
    .orderBy(col("Average_IMDB_Rating"))

  averageIMDBDF.show()


}
