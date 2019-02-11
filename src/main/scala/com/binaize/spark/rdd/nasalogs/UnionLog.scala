package com.binaize.spark.rdd.nasalogs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object UnionLog extends Serializable {

  def main(args: Array[String]) {

    /* "input/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "input/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "output/sample_nasa_logs.tsv" file.

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

    val unionLog = julyLogWithoutHeader.union(augLogWithoutHeader)

    val sample = unionLog.sample(false, 0.1, 102)

    for (x <- sample.collect())
      println(x)

    sample.saveAsTextFile("data/output/sample_nasa_logs.tsv")

  }
}