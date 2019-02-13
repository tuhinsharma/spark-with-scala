package com.binaize.spark.pairedRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object JoinOperations {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val partitioner = new HashPartitioner(20)

    val ages = sc.parallelize(List(("Tom", 29), ("John", 22)))
    ages.partitionBy(partitioner)
    val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))
    addresses.partitionBy(partitioner)

    val join = ages.join(addresses)
    join.saveAsTextFile("output/age_address_join.text")

    val leftOuterJoin = ages.leftOuterJoin(addresses)
    leftOuterJoin.saveAsTextFile("output/age_address_left_out_join.text")

    val rightOuterJoin = ages.rightOuterJoin(addresses)
    rightOuterJoin.saveAsTextFile("output/age_address_right_out_join.text")

    val fullOuterJoin = ages.fullOuterJoin(addresses)
    fullOuterJoin.saveAsTextFile("output/age_address_full_out_join.text")
  }
}
