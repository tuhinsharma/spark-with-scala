package com.binaize.spark.rdd.numbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SumOfNumbers {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from input/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "SumOfNumbers")
      .getOrCreate()
    val sc = spark.sparkContext
    val primeNumbers = sc.textFile(path = "data/input/prime_nums.text")
    val sum = primeNumbers.flatMap(line => line.split("\\s+")).filter(line => !line.isEmpty).map(line => line.toInt).reduce((x, y) => x + y)
    println(sum)
  }
}