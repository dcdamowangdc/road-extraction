package main

import comm.Spark_Session.spark
import comm.gpsUtil.{cal_move_x, cal_move_y, get_distance, get_p, get_relative_x, get_relative_y, lat_to_y, lon_to_x, x_to_lon, y_to_lat}
import process.ProcessStep.getTime
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, lit, sum, when}

object loopCal {
  def main(args: Array[String]): Unit = {

    var a = 0
    var b = 30

    if(args.length>0){
      a = args(0).toInt
      b = args(1).toInt
    }

    val compGpsTn = "comp_gps2"
    val gpsRelTn = s"gps_rel"
    val processGpsTn = s"process_gps"

    var gpsRelA = spark.read.option("header", "true").csv("./data/gpsRel")
      .select("rcrd_time", "drc0", "rcrd_timea")
    gpsRelA=gpsRelA.cache()

    val t1 = System.nanoTime()

    for(n<- a to b){
      val start_time=System.nanoTime()
      var gpsData = spark.emptyDataFrame
      var gpsData2 = spark.emptyDataFrame

      // 第一次从原始压缩后的表 comp_gps 选数据，之后从过程表 process_gps 选数据
      if (n==0){
        // gpsData = spark.sql(s"select rcrd_time,lo_lgt,lo_ltt from bigdata_test.$compGpsTn")
        gpsData = spark.read.option("header", "true").csv("./data/gpsRel")
          .select("rcrd_time", "lo_lgt", "lo_ltt")
          .withColumn("lo_lgt", lon_to_x(col("lo_lgt")))
          .withColumn("lo_ltt", lat_to_y(col("lo_ltt")))
        gpsData2 = gpsData
      }else{
        // gpsData = spark.sql(s"select rcrd_time,lo_lgt,lo_ltt from bigdata_test.$processGpsTn where flag='$n'")
        gpsData = spark.read.option("header", "true").csv(s"./data/processGps$n")
        gpsData2 = gpsData
      }

      gpsData=gpsData.withColumnRenamed("lo_lgt","x0")
        .withColumnRenamed("lo_ltt","y0")

      gpsData2=gpsData2.withColumnRenamed("rcrd_time","rcrd_timea")
        .withColumnRenamed("lo_lgt","xa")
        .withColumnRenamed("lo_ltt","ya")

      val joinGpsA=gpsRelA.join(gpsData, Seq("rcrd_time"),"inner")
        .join(gpsData2, Seq("rcrd_timea"), "inner")
        .withColumn("dist", get_distance(x_to_lon(col("x0")),y_to_lat(col("y0")),x_to_lon(col("xa")),y_to_lat(col("ya"))))
        .filter(col("dist")<=15)
        .drop("dist")

      joinGpsA.show(10, truncate = false)
      cal(joinGpsA, n, processGpsTn)

      val run_time2 = (System.nanoTime() - start_time) / 1e9 / 60.0
      println(s"$n run time " + f"$run_time2%1.2f" + " minutes")
    }

    val t2 = System.nanoTime()
    println("Avg run time " + f"${getTime(t1, t2)/(b-a+1)}%1.2f" + " minutes")
    println("Total run time " + f"${getTime(t1, t2)}%1.2f" + " minutes")

  }

  def cal(joinGpsA:DataFrame, n:Int, processGpsTn:String): Unit ={
    println(s"开始计算弟$n 次")
    var joinGps = joinGpsA
    joinGps=joinGps.withColumn("xa1", get_relative_x(col("xa"), col("ya"), col("x0"), col("y0"), col("drc0")))
      .withColumn("ya1", get_relative_y(col("xa"), col("ya"), col("x0"), col("y0"), col("drc0")))
      .withColumn("x_pos", when(col("xa1") > 0, lit(1)).when(col("xa1") < 0, lit(-1)).otherwise(0))
      // 2、计算权重，p: 距离, 距离越大，权重越小
      .withColumn("p", get_p(col("xa"),col("ya")))
    //.withColumn("p", when(col("ya1")>0,col("ya1")).otherwise(-col("ya1")))

    // 计算x轴正负方向上各个量的权重 v_pow
    val key_col = List("rcrd_time", "x_pos")

    val sumpDf = joinGps.groupBy(key_col.map(x => col(x)): _*)
      .agg(sum("p").alias("sump"))
    joinGps=joinGps.join(sumpDf, Seq("rcrd_time", "x_pos"), "left")
      .withColumn("v_pow", lit(1) - col("p")/col("sump"))

    // v_pom归一化 v_pom=v_pow/sum_pow
    val sumpowDf = joinGps.groupBy(key_col.map(x => col(x)): _*)
      .agg(sum("v_pow").alias("sum_pow"))
    joinGps = joinGps.join(sumpowDf, Seq("rcrd_time", "x_pos"), "left")
      .withColumn("v_pow", col("v_pow")/col("sum_pow"))
      .drop("p", "sump", "sum_pow")
      .withColumn("dx", col("xa1")*col("v_pow"))

    // 正负方向的dx分别求均值 再相加
    val key_col2 = List("rcrd_time", "x_pos", "x0", "y0","drc0")
    joinGps = joinGps.groupBy(key_col2.map(x => col(x)): _*)
      .agg(avg("dx").alias("dx"))
      .withColumn("dx", when(col("dx").isNotNull, col("dx")).otherwise(0))
      .groupBy("rcrd_time", "x0", "y0", "drc0")
      .agg(sum("dx").alias("dx"))
      // 修改x0 y0， 改为修改 car_no rcrd_time 对应的坐标
      .withColumn("x0", cal_move_x(col("x0"), col("dx"), col("drc0")))
      .withColumn("y0", cal_move_y(col("y0"), col("dx"), col("drc0")))
      .drop("dx", "drc0")
      .withColumnRenamed("x0", "lo_lgt")
      .withColumnRenamed("y0", "lo_ltt")
      .withColumn("flag", lit(n+1))

    println("开始插入表")
//    hive_insert_part(joinGps, processGpsTn, "flag")
  }

}
