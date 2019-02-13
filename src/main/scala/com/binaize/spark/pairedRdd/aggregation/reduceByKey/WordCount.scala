package com.binaize.spark.pairedRdd.filter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.binaize.spark.commons.Utils

object WordCount {

  def main(args: Array[String]) {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val lines = sc.textFile(path = "data/input/word_count.text")
    lines.persist()
    val words = lines.flatMap(line => line.split(" "))
    val wordPair = words.map(line => (line, 1))
    val wordCount = wordPair.reduceByKey((x, y) => x + y)
    for (x <- wordCount.collect())
      println(x)
  }
}
