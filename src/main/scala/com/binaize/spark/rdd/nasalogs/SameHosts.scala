package com.binaize.spark.rdd.nasalogs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SameHosts {

  def main(args: Array[String]) {

    /* "input/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "input/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val julyLog = sc.textFile("data/input/nasa_19950701.tsv")
    val julyLogWithoutHeader = julyLog.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val augLog = sc.textFile("data/input/nasa_19950801.tsv")
    val augLogWithoutHeader = augLog.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    val intersectionLog = julyLogWithoutHeader.map(line => line.split("\t")(0)).intersection(augLogWithoutHeader.map(line => line.split("\t")(0)))
    for (x <- intersectionLog.collect()) println(x)

    intersectionLog.saveAsTextFile("output/nasa_logs_same_hosts.csv")

  }
}
