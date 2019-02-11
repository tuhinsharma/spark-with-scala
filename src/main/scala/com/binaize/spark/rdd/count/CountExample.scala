package com.binaize.spark.rdd.count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CountExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)
    println("Count: " + wordRdd.count())

    val wordCountByValue = wordRdd.countByValue()
    println("CountByValue:")

    for ((word, count) <- wordCountByValue) println(word + " : " + count)
  }
}
