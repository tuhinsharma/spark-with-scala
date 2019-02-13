package com.binaize.spark.advanced.accumulator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.binaize.spark.commons.Utils

object StackOverFlowSurvey {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val total = sc.longAccumulator
    val missingSalaryMidPoint = sc.longAccumulator

    val responseRDD = sc.textFile("data/input/2016-stack-overflow-survey-responses.csv")

    val responseFromCanada = responseRDD.filter(response => {
      val splits = response.split(Utils.COMMA_DELIMITER, -1)
      total.add(1)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }

      splits(2) == "Canada"
    })

    println("Count of responses from Canada: " + responseFromCanada.count())
    println("Total count of responses: " + total.value)
    println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value)
  }
}
