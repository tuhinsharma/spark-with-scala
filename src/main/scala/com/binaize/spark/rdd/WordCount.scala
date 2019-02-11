package com.binaize.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[2]")
      .appName("orgfalkonsparkrddWordCount")
      .getOrCreate()
    val sc = spark.sparkContext

    val lines = sc.textFile(path="data/input/word_count.text")
    lines.persist()
    val words = lines.flatMap(line => line.split(" "))
    for (x <- words.collect())
      println(x)

    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts)
      println(word + " : " + count)
  }
}
