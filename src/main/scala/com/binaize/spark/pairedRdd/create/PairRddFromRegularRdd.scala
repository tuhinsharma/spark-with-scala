package com.sparkTutorial.pairRdd.create

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PairRddFromRegularRdd {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputStrings = List("Lily 23", "Jack 29", "Mary 29", "James 8")
    val regularRDDs = sc.parallelize(inputStrings)

    val pairRDD = regularRDDs.map(s => (s.split(" ")(0), s.split(" ")(1)))
    pairRDD.coalesce(1).saveAsTextFile("data/output/pair_rdd_from_regular_rdd")
  }
}
