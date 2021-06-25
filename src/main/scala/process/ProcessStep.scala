package process

import comm.Spark_Session.spark
import comm.gpsUtil.{getDistance, getDrc, get_distance, get_drc, to_r, x_to_lon}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lag, lit, substring, unix_timestamp, when}

case class Gps(rcrd_date: String, car_no: String, rcrd_time: String, lo_lgt: String, lo_ltt: String, lo_drc: String, time_diff: String, var r_num: String)
object ProcessStep {
  def compressAndCalRel(v_gpsData:DataFrame):Unit = {

    val compGpsTn = "comp_gps" // tyData的gps数据压缩后的表
    val gpsRelTn = "gps_rel" // gps点之间相似的关系表

    val rNumTime = 300.0 // 根据300秒的时间间隔划分车辆行驶的趟次
    val maxDist = 10.0 // 当角度差小于R=5°时，保留距离至少maxDist=10米的点
    val R = 5.0

    val t1 = System.nanoTime()
    val w1 = Window.partitionBy("car_no").orderBy("RCRD_TIME")
    v_gpsData.createOrReplaceTempView("gps_data")

    var gpsData2 = spark.sql(
      s"""
         |select t.rcrd_date,
         | t.car_no,
         | t.rcrd_time,
         | t.lo_lgt,
         | t.lo_ltt,
         | t.lo_drc,
         | row_number() over(partition by car_no,substr(rcrd_time,1,19) order by rcrd_time) nn
         |from gps_data t
         |where t.lo_lgt is not null
         |and t.lo_ltt is not null
         |and t.lo_drc is not null
         |order by t.rcrd_time
         |""".stripMargin)
      .filter(col("nn") === lit(1)).drop("nn")

    gpsData2 = gpsData2.withColumn("Time", unix_timestamp(gpsData2("RCRD_TIME")) + substring(gpsData2("RCRD_TIME"), 21, 3) / 1000)
      .withColumn("time_diff", col("Time") - lag(col("Time"), 1).over(w1))
      .withColumn("time_diff", when(col("time_diff").isNotNull,col("time_diff")).otherwise(0))
      .withColumn("r_num", lit(0))
      .drop("Time")

    println(s"总数：", gpsData2.count())

    import spark.implicits._
    val gpsData: Array[Gps] = gpsData2.as[Gps].collect()

    // 压缩
    var rList: List[Gps] = List()
    var curr = 0
    for (s <- gpsData.indices) {
      if (s == curr) {
        rList = rList :+ gpsData(s)
        curr = s + 1
        if (curr < gpsData.length - 1) {
          var dist = getDistance(gpsData(s).lo_lgt.toDouble, gpsData(s).lo_ltt.toDouble, gpsData(curr).lo_lgt.toDouble, gpsData(curr).lo_ltt.toDouble)
          var diffDrc = getDrc(gpsData(s).lo_drc.toDouble, gpsData(curr).lo_drc.toDouble)

          while (dist < maxDist && diffDrc < R && curr < gpsData.length - 2) {
            curr = curr + 1
            dist = getDistance(gpsData(s).lo_lgt.toDouble, gpsData(s).lo_ltt.toDouble, gpsData(curr).lo_lgt.toDouble, gpsData(curr).lo_ltt.toDouble)
            diffDrc = getDrc(gpsData(s).lo_drc.toDouble, gpsData(curr).lo_drc.toDouble)
          }
        }
      }
    }

    // 每趟标记 r_num
    var r = 0
    for (s <- rList.indices) {
      if(gpsData(s).time_diff.toDouble > rNumTime) r += 1
      gpsData(s).r_num = r.toString
    }

    val compGps = rList.toDF().drop("time_diff")
    compGps.show(10)
    println(s"压缩后的长度：${rList.length}")

    val saveOption1 = Map("header" -> "true", "path" -> "./data/compGps")
    println("    写入compGps")
    compGps.coalesce(1).write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite).options(saveOption1).save()

//    val v_compGps = compGps.select("car_no", "rcrd_time", "lo_lgt", "lo_ltt", "lo_drc")
//    calJoin(v_compGps, R)

    // 计算GPS点的相似关系
  }

  def calJoin(v_compGps:DataFrame, R:Double):DataFrame={
    val dataDf = v_compGps

    val gpsData1 = dataDf.withColumnRenamed("lo_lgt","x0")
      .withColumnRenamed("lo_ltt","y0")
      .withColumnRenamed("lo_drc", "drc0")

    val gpsData2 = dataDf.withColumnRenamed("rcrd_time","rcrd_timea")
      .withColumnRenamed("car_no", "car_noa")
      .withColumnRenamed("lo_lgt","xa")
      .withColumnRenamed("lo_ltt","ya")
      .withColumnRenamed("lo_drc","drca")

    // 分类聚合
    // 40米内都属于缓冲区中，取15米内的点计算
    //      get_distance(gpsData1("x0"),gpsData1("y0"),gpsData2("xa"),gpsData2("ya"))<=40
    val gpsRel = gpsData1.join(gpsData2,
      get_distance(gpsData1("x0"),gpsData1("y0"),gpsData2("xa"),gpsData2("ya"))<=30
        &&get_drc(gpsData1("drc0"),gpsData2("drca"))<=R,"left")
      .withColumn("dist", get_distance(gpsData1("x0"),gpsData1("y0"),gpsData2("xa"),gpsData2("ya")))
      .withColumn("dist15", when(col("dist")<=15,1).otherwise(0))
      .withColumn("drc0", to_r(col("drc0")))
      .drop("x0", "y0", "xa", "ya", "drca", "dist")
    val saveOption2 = Map("header" -> "true", "path" -> "./data/gpsRel")

    println("    写入gpsRel")
    gpsRel.coalesce(1).write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite).options(saveOption2).save()

    gpsRel
  }

  def gpsTail(){
//    val t1 = System.nanoTime()
    val compGpsTn = s"comp_gps"
    val gpsRelTn = s"gps_rel"
    val compGpsTn2 = s"comp_gps2"

    val compGps = spark.sql(
      s"""
         |select car_no,rcrd_time,r_num from bigdata_test.${compGpsTn} a
         |""".stripMargin)

    // mm = 1 则保留
    val dist15GpsRel = spark.sql(
      s"""
         |select car_no,rcrd_time,count(*) as nn from bigdata_test.$gpsRelTn b
         |where dist15=1
         |group by car_no, rcrd_time
         |""".stripMargin)
      .withColumn("mm", when(col("nn")>=7, 1).otherwise(0))
      .drop("nn")

    val dist15GpsRel1 = dist15GpsRel.filter(col("mm")===1)
    val dist15GpsRel0 = dist15GpsRel.filter(col("mm")===0)

    val gps = compGps.join(dist15GpsRel0, Seq("car_no", "rcrd_time"), "inner")

    //  r_num  dist15n   1,2,3,5
    // r_num 中 超过100个点位为异常点的轨迹删除
    gps.groupBy("r_num").agg(count("mm").alias("dist15n"))
      .filter(col("dist15n")<100).createOrReplaceTempView("rnum_t")

    val compGps2 = spark.sql(
      s"""
         |select * from bigdata_test.$compGpsTn
         |""".stripMargin)
      .join(dist15GpsRel1,Seq("car_no", "rcrd_time"), "inner")
      .drop("mm")

//    hive_insert_dc(compGps2, compGpsTn2)
//    val t2 = System.nanoTime()
//    println("compGps2 run time " + f"${getTime(t1, t2)}%1.2f" + " minutes")

  }

  def getTime(t1:Long, t2:Long):Double={
    (t2-t1)/ 1e9 / 60.0
  }

}
