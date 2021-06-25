package main

import comm.Spark_Session.spark

object test1 {
  def main(args: Array[String]): Unit = {
    spark.read.option("header", "true").csv("./data/compGps").show(10)
  }

}
