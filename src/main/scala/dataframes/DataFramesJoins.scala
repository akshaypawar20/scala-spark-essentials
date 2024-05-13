package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataFramesJoins extends App{

  val spark = SparkSession.builder()
    .appName("DataFramesJoins")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  // INNER JOIN
  val guitarBandsInnerDF = bandsDF.join(guitarsDF, bandsDF.col("id") === guitarsDF.col("band"), "inner").drop(guitarsDF.col("id"))

  // LEFT JOIN
  val guitarBandsLeftDF = bandsDF.join(guitarsDF, bandsDF.col("id") === guitarsDF.col("band"), "left").drop(guitarsDF.col("id"))

  // RIGHT JOIN
  val guitarBandsRightDF = bandsDF.join(guitarsDF, bandsDF.col("id") === guitarsDF.col("band"), "right").drop(guitarsDF.col("id"))

  // OUTER JOIN
  val guitarBandsOuterDF = bandsDF.join(guitarsDF, bandsDF.col("id") === guitarsDF.col("band"), "outer").drop(guitarsDF.col("id"))

  // LEFT SEMI JOIN
  val guitarBandsLeftSemiDF = bandsDF.join(guitarsDF, bandsDF.col("id") === guitarsDF.col("band"), "leftsemi")

  // LEFT ANTI JOIN
  val guitarBandsLeftAntiDF = bandsDF.join(guitarsDF, bandsDF.col("id") === guitarsDF.col("band"), "leftanti")


}
