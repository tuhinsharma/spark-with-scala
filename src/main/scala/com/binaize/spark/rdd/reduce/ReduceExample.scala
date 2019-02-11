package com.binaize.spark.rdd.reduce

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReduceExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputIntegers = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputIntegers)

    val product = integerRdd.reduce((x, y) => x * y)
    println("product is :" + product)
  }
}
