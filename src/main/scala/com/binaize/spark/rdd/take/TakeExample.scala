package com.binaize.spark.rdd.take

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TakeExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)

    val words = wordRdd.take(3)
    for (word <- words) println(word)
  }
}
