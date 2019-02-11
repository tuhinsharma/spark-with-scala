package com.binaize.spark.pairedRdd.filter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.binaize.spark.commons.Utils

object AirportsNotInUsa {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val airportsRDD = sc.textFile("data/input/airports.text")

    val airportPairRDD = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(3)))
    val airportsNotInUSA = airportPairRDD.filter(keyValue => keyValue._2 != "\"United States\"")

    airportsNotInUSA.saveAsTextFile("data/output/airports_not_in_usa_pair_rdd.text")
  }
}
