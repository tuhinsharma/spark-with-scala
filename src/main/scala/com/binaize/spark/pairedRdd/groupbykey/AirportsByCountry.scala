package com.binaize.spark.pairedRdd.groupbykey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.binaize.spark.commons.Utils

object AirportsByCountry {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from data/input/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val lines = sc.textFile("data/input/airports.text")

    val countryAndAirportNameAndPair = lines.map(airport => (airport.split(Utils.COMMA_DELIMITER)(3),
      airport.split(Utils.COMMA_DELIMITER)(1)))

    val airportsByCountry = countryAndAirportNameAndPair.groupByKey()

    for ((country, airportName) <- airportsByCountry.collectAsMap()) println(country + ": " + airportName.toList)
  }
}
