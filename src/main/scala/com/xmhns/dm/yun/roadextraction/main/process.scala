package com.xmhns.dm.yun.roadextraction.main

import com.xmhns.dm.yun.roadextraction.comm.Spark_Session.spark
import com.xmhns.dm.yun.roadextraction.comm.hive_comm.hive_insert_dc
import com.xmhns.dm.yun.roadextraction.process.ProcessStep

object process {
  def main(args: Array[String]): Unit = {
    // 起止日期，一般取三天
    val startDate = args(0)
    val endDate = args(1)
    // 公司缩写 corp_short_name
    val company = args(2)
    // 车牌号
    val lic_plt_no = args(3)

    val tableName = "gps_route"
    val start_time = System.nanoTime()
    // 1、获取Ty的gps轨迹数据
    val ty_data = spark.sql(
      s"""
         |select
         | rcrd_date,
         | lic_plt_no as car_no,
         | up_dn,
         | rcrd_time,
         | can_speed,
         | can_mile,
         | lo_lgt,
         | lo_ltt,
         | lo_drc
         |from hnscan_comm.t_sd_ty_info
         |where dt >= '$startDate'
         |and dt <= '$endDate'
         |and company = '$company'
         |and lic_plt_no = '$lic_plt_no'
         |""".stripMargin)

    println(s"开始插入表$tableName:")
    hive_insert_dc(ty_data, tableName)
    println(s"ty_data num: ${ty_data.count()}")

    // 2、压缩并计算GPS关系
    ProcessStep.compressAndCalRel(tableName)

    val run_time = (System.nanoTime() - start_time) / 1e9 / 60.0
    println("Total run time " + f"$run_time%1.2f" + " minutes")

    // 3、循环迭代进行引力压缩计算
    // loopCal
  }

}
