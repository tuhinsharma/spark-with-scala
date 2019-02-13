package com.binaize.spark.pairedRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SortedWordCount {

  def main(args: Array[String]) {
    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val lines = sc.textFile(path = "data/input/word_count.text")
    lines.persist()

    val words = lines.flatMap(line => line.split(" "))
    val wordCountPair = words.map(word => (word, 1))

    val wordCount = wordCountPair.reduceByKey((x, y) => (x + y))
    val countWord = wordCount.map(line => (line._2, line._1))

    val sortedCountWord = countWord.sortByKey(false)
    val sortedWordCount = sortedCountWord.map(line => (line._2, line._1))
    for (x <- sortedWordCount.collect())
      println(x._1 + " : " + x._2)


  }
}

