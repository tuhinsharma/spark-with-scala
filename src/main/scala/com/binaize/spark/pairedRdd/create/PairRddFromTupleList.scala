package com.sparkTutorial.pairRdd.create

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PairRddFromTupleList {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))
    val pairRDD = sc.parallelize(tuple)

    pairRDD.repartition(1).saveAsTextFile("data/output/pair_rdd_from_tuple_list")
  }
}
