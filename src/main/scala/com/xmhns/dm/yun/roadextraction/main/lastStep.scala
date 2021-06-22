package com.xmhns.dm.yun.roadextraction.main

import com.xmhns.dm.yun.roadextraction.comm.Spark_Session.spark
import com.xmhns.dm.yun.roadextraction.comm.gpsUtil.{getDegree, wgs_to_gjc, x_to_lon, y_to_lat}
import com.xmhns.dm.yun.roadextraction.comm.hive_comm.{hive_insert_part, oracle_insert}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, lit}

import scala.collection.mutable.ArrayBuffer

case class gpsPoint(rcrd_time: String, lo_lgt: Double, lo_ltt: Double, var flag: String, r_num: String)
object lastStep {
  def main(args: Array[String]): Unit = {

    // 参数
    val oracleTableName = "t_mod_lineinfo"
    val hive_database = "bigdata_test."
    val gpsRoute = "gps_route"
    val start_time = System.nanoTime()

    // 给聚合后的flag=31的gps轨迹数据关联上r_num
    spark.sql(
      """
        |SELECT a.*,b.r_num
        |    FROM bigdata_test.process_gps a
        |    INNER JOIN bigdata_test.comp_gps2 b
        |    on a.rcrd_time=b.rcrd_time
        |    WHERE a.flag= 31
        |    AND a.lo_lgt is not null
        |""".stripMargin)
      .withColumn("lo_lgt", x_to_lon(col("lo_lgt")))
      .withColumn("lo_ltt", y_to_lat(col("lo_ltt")))
      .createOrReplaceTempView("gps_data")

    val rows = spark.sql(
      """
        |SELECT *
        |FROM
        |(
        | SELECT r_num,
        | count(*) as gps_num
        | FROM gps_data
        | group by r_num
        |)
        | WHERE gps_num<3000 AND gps_num>1500
        | ORDER BY gps_num
        |""".stripMargin).collect()

    for(i <- rows.indices){
      println(rows(i))
    }

    val r = rows(rows.length/2)(0)

    val df = spark.sql(
      s"""
         |SELECT * FROM gpsdata
         | WHERE r_num = $r
         | ORDER BY rcrd_time
         |""".stripMargin)

    println(s"r_num:$r count: ${df.count()}")

    import spark.implicits._
    val data = df.as[gpsPoint].collect()

    // 去锯齿点
    var rList: List[gpsPoint] = List()
    for(i <- 1 until data.length-2){
      val a = getDegree(data(i-1).lo_ltt, data(i-1).lo_lgt, data(i).lo_ltt, data(i).lo_lgt)
      val b = getDegree(data(i).lo_ltt, data(i).lo_lgt, data(i+1).lo_ltt, data(i+1).lo_lgt)
      if(math.abs(a-b)<=40.0) rList = rList :+  data(i)
    }

    println("去锯齿点后count:" + rList.toDF().count())

    // DP压缩
    val data2 = rList.toDF().as[gpsPoint]
    DP_compress(data2.collect(), 7)
    var dpData = output_point_list.toList.toDF().dropDuplicates().orderBy("rcrd_time")
    println("DP压缩后count:" + dpData.count())

    val car_no = spark.sql(
      s"""
         |SELECT distinct car_no FROM bigdata_test.$gpsRoute
         |""".stripMargin).take(1)(0).getString(0)

    val car_info = spark.sql(
      s"""
         |SELECT a.lic_plt_no as car_no ,a.corp_id ,b.line_id
         |FROM hnscan_business.t_base_car_info a,hnscan_business.t_base_line_car_rel b
         |WHERE a.lic_plt_no = '$car_no'
         |AND a.car_id =b.car_id
         |AND a.corp_id =b.corp_id
         |""".stripMargin).take(1)(0)

    val corp_id = car_info(1)
    val line_id = car_info(2)
    println(s"car_no:$car_no, corp_id:$corp_id, line_id:$line_id")

    val winPart = Window.partitionBy("flag","r_num").orderBy("rcrd_time")
    dpData=dpData.withColumnRenamed("lo_lgt","start_lgt")
      .withColumnRenamed("lo_ltt","start_ltt")
      .withColumn("end_lgt", lead(col("start_lgt"),1).over(winPart))
      .withColumn("end_ltt", lead(col("start_ltt"),1).over(winPart))
      .withColumn("corp_id", lit(corp_id))
      .withColumn("line_id", lit(line_id))
      .withColumn("start_lgt2", wgs_to_gjc(col("start_lgt"), col("start_ltt"))(0))
      .withColumn("start_ltt2", wgs_to_gjc(col("start_lgt"), col("start_ltt"))(1))
      .withColumn("end_lgt2", lead(col("start_lgt2"),1).over(winPart))
      .withColumn("end_ltt2", lead(col("start_ltt2"),1).over(winPart))

    dpData.createTempView("dp_data")
    // 输出表t_mod_lineinfo的表结构
    val outDf = spark.sql(
      """
        |SELECT
        |  corp_id as company,
        |  line_id,
        |  0 as up_dn,
        |  row_number() over(partition by corp_id,line_id ORDER BY rcrd_time) as road_id,
        |  start_lgt,
        |  start_ltt,
        |  end_lgt,
        |  end_ltt,
        |  start_lgt2,
        |  start_ltt2,
        |  end_lgt2,
        |  end_ltt2
        |FROM dp_data
        |""".stripMargin)

    println("开始插入hive的表bigdata_test.t_mode_lineinfo")
    hive_insert_part(outDf, "t_mod_lineinfo", "company, line_id", hive_database)

    println("开始插入oracle的表t_mode_lineinfo")
    oracle_insert(outDf, oracleTableName, "t_mod_lineinfo")

    val run_time = (System.nanoTime() - start_time) / 1e9 / 60.0
    println("Total run time " + f"$run_time%1.2f" + " minutes")
  }



  private var output_point_list:ArrayBuffer[gpsPoint] = ArrayBuffer()

  def DP_compress(point_list:Array[gpsPoint], Dmax:Double):Unit={
    val start_index = 0
    val end_index = point_list.length - 1
    // 起止点必定是关键点,但是作为递归程序此步引入了冗余数据,后期必须去除
    output_point_list = output_point_list :+ point_list(start_index) :+ point_list(end_index)

    if (start_index < end_index){
      var index = start_index + 1
      var max_vertical_dist = 0.0  //路径中离弦最远的距离
      var key_point_index = 0  //路径中离弦最远的点,即划分点

      while (index < end_index){
        val cur_vertical_dist = get_vertical_dist(point_list(start_index), point_list(end_index), point_list(index))
        if (cur_vertical_dist > max_vertical_dist){
          max_vertical_dist = cur_vertical_dist
          key_point_index = index  //记录划分点
        }
        index += 1
      }
      //      递归划分路径
      if(max_vertical_dist >= Dmax){
        DP_compress(point_list.slice(start_index, key_point_index), Dmax)
        DP_compress(point_list.slice(key_point_index, end_index), Dmax)
      }
    }
  }

  def Rad(d:Double):Double={
    d * math.Pi / 180.0
  }

  def Geodist(point1:gpsPoint, point2:gpsPoint):Double = {
    val radLat1 = Rad(point1.lo_ltt)
    val radLat2 = Rad(point2.lo_ltt)
    val delta_lon = Rad(point1.lo_lgt - point2.lo_lgt)
    val top_1 = math.cos(radLat2) * math.sin(delta_lon)
    val top_2 = math.cos(radLat1) * math.sin(radLat2) - math.sin(radLat1) * math.cos(radLat2) * math.cos(delta_lon)
    val top = math.sqrt(top_1 * top_1 + top_2 * top_2)
    val bottom = math.sin(radLat1) * math.sin(radLat2) + math.cos(radLat1) * math.cos(radLat2) * math.cos(delta_lon)
    val delta_sigma = math.atan2(top, bottom)
    val distance = delta_sigma * 6378137.0
    distance.formatted("%.3f").toDouble
  }

  // 2.点弦距离
  def get_vertical_dist(pointA:gpsPoint, pointB:gpsPoint, pointX:gpsPoint):Double={
    val a = math.abs(Geodist(pointA, pointB))
    if (a.equals(0.0)) return math.abs(Geodist(pointA, pointX))
    val b = math.abs(Geodist(pointA, pointX))
    val c = math.abs(Geodist(pointB, pointX))
    val p = (a + b + c) / 2
    val S = math.sqrt(math.abs(p * (p - a) * (p - b) * (p - c)))
    S * 2 / a
  }
}