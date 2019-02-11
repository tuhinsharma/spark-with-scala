package com.binaize.spark.rdd.airports

import com.binaize.spark.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AirportsByLatitude {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from input/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to output/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext
    val airports = sc.textFile(path = "data/input/airports.text")
    val airportsLatitudes = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)
    val airportsNameAndLatitude = airportsLatitudes.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + "," + splits(6)
    })
    for (x <- airportsNameAndLatitude.collect())
      println(x)
    airportsNameAndLatitude.saveAsTextFile("data/output/airports_by_latitude.text")
  }
}