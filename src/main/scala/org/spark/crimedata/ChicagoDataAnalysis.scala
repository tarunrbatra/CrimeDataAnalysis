package org.spark.crimedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * This program will read the crime data from Chicago data portal and try to find count of each crime for each Month since 2001
 * So the output will be something like this-
 * Month Crime-Type Count
 * Jan   BURGLARY   1000
 */

// Data present at https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present

object ChicagoDataAnalysis extends App {

  // create spark session - change master if running in cluster mode
  val spark = SparkSession
    .builder()
    .appName("ChicagoDataAnalysis")
    .master("local[1]")
    .getOrCreate()

  // creating DataFrame from crime data csv

  val inputDF = spark
    .read
    .option("header", true)
    .csv("src/main/resources/Crimes_-_2001_to_Present.csv")

  inputDF.show()

  // only need Date and Type of Crime
  val relevantDataDF = inputDF.select("Date", "Primary Type")
    .withColumnRenamed("Primary Type", "CrimeType")

  val DateAsTimeStampDF  = relevantDataDF.withColumn("Date_TS", to_timestamp(col("Date"), "MM/dd/yyyy HH:mm:ss"))
  //05/31/2007 08:15:00
  DateAsTimeStampDF.show()
  DateAsTimeStampDF.printSchema()

  val DFWithYearCol  = DateAsTimeStampDF.withColumn("Year", year(col("Date_TS")))
    .withColumn("Month", month(col("Date_TS")))
  DFWithYearCol.show()

  DFWithYearCol.groupBy("Year","Month", "CrimeType").count().orderBy(col("Year").desc, col("Month").desc, col("count").desc).show(100)

}
