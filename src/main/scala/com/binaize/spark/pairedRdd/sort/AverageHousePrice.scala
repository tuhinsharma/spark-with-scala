package com.binaize.spark.pairedRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AverageHousePrice {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder().config("spark.master", "local[*]")
      .appName(name = "combinaizesparkrddAirportsInUsaProblem")
      .getOrCreate()
    val sc = spark.sparkContext

    val realEstate = sc.textFile("data/input/RealEstate.csv")
    val header = realEstate.first()
    val realEstateWithoutHeader = realEstate.filter(line => line != header)
    val bedroomPricePair = realEstateWithoutHeader.map(line => {
      val splits = line.split(",")
      (splits(3).toInt, (1.0, splits(2).toDouble))
    })
    val bedroomCountPricePair = bedroomPricePair.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val bedroomAveragePrice = bedroomCountPricePair.mapValues(line => line._2 / line._1)

    val sortedBedroomAveragePrice = bedroomAveragePrice.sortByKey(ascending = false)
    for (x <- sortedBedroomAveragePrice.collect())
      println(x)
  }

}
