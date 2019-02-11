package com.binaize.spark.rdd.airports

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.binaize.spark.commons.Utils

object AirportsInUsa {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from input/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to output/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName("combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext
    val airports = sc.textFile(path = "data/input/airports.text")
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")
    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + "," + splits(2)
    })
    for (x <- airportsNameAndCityNames.collect())
      println(x)
    airportsNameAndCityNames.saveAsTextFile(path = "data/output/airports_in_usa.text")
  }
}
