package main
import comm.Spark_Session.spark
import process.ProcessStep


object Step1 {
  def main(args: Array[String]): Unit = {
    // 1、获取Ty的gps轨迹数据
    val gpsData = spark.read.option("header", "true").csv("./data/gps.csv")
    // 2、压缩并计算GPS关系
    ProcessStep.compressAndCalRel(gpsData)

//    val run_time = (System.nanoTime() - start_time) / 1e9 / 60.0
//    println("Total run time " + f"$run_time%1.2f" + " minutes")

    // 3、循环迭代进行引力压缩计算
    // loopCal
  }

}
