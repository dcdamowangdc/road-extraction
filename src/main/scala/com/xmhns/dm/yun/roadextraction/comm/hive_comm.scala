package com.xmhns.dm.yun.roadextraction.comm

import com.xmhns.dm.yun.roadextraction.comm.Spark_Session.spark
import com.xmhns.dm.yun.roadextraction.comm.jdbcUtil.DeleteRow
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, substring}

object hive_comm {
  def hive_insert_dc(df:DataFrame,table_name:String,db:String="bigdata_test."):Unit={
    //在集群中写入Hive数据库
    val df2 =df.repartition(1)
    val temp_table = table_name
    df2.createOrReplaceTempView(temp_table)
    val col_names = spark.sql("describe "+db+table_name).select("col_name").where(substring(col("col_name"),0,1)=!="#").
      collect().flatMap(x=>List(x.get(0))).distinct.mkString(",")
    spark.sql("insert overwrite table "+db+table_name + " select " + col_names+" from "+temp_table)
  }

  def hive_insert_part(df:DataFrame,table_name:String,partition_field:String="corp_id,rcrd_date",db:String="bigdata_test."):Unit={
    //在集群中写入Hive数据库
    val df2 = df.repartition(1)
    val temp_table = table_name + "_temp"
    df2.createOrReplaceTempView(temp_table)
    spark.sql("refresh " + db + table_name)
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.execution.engine=tez")
    val col_names = spark.sql("describe "+ db + table_name).select("col_name").where(substring(col("col_name"),0,1)=!="#").
      collect().flatMap(x=>List(x.get(0))).distinct.mkString(",")
    spark.sql("insert overwrite table "+db+table_name+" partition("+partition_field+") " +
      "select "+col_names+" from "+temp_table)
  }

  def oracle_insert(df: DataFrame, oracleTableName: String, hiveTableName: String): Unit = {
    println("oracle insert: " + oracleTableName)
    val o_driver = "oracle.jdbc.driver.OracleDriver"
//     oracle测试库
        val o_url = "jdbc:oracle:thin:@192.168.70.200:1521:hnscan"
        val user = "CAN4_CS"
        val password = "CAN4_CS"

    // oracle生产库
//    val o_url = "jdbc:oracle:thin:@192.168.12.185:1521/prodb"
//    val user = "Can"
//    val password = "HnsCAN2014"

    val jdbc_map = Map(
      "driver" -> o_driver,
      "url" -> o_url,
      "user" -> user,
      "password" -> password)
    // 获取公司ID和线路ID，判断删除相同的数据
    val corp_line = df.select("company", "line_id").distinct().take(1)(0)
    val del_sql =
      s"""
        |delete from $oracleTableName where company = '${corp_line(0)}' and line_id = ${corp_line(1)}
        |""".stripMargin
    DeleteRow(o_url, user, password, del_sql)

    //过滤掉多余的字段
    val temp_table = oracleTableName + "_temp"
    df.createOrReplaceTempView(temp_table)

    val col_names = spark.sql("describe bigdata_test." + hiveTableName)
      .select("col_name")
      .where(substring(col("col_name"), 0, 1) =!= "#")
      .collect().flatMap(x => List(x.get(0))).distinct.mkString(",")

    val dataFrame: DataFrame = spark.sql(" select " + col_names + " from " + temp_table)
    //再插入到数据库
    dataFrame.repartition(1).write.format("jdbc").options(jdbc_map + ("dbtable" -> oracleTableName))
      .option("batchsize", 10000)
      .mode("append").save()
    println("finish! " + oracleTableName)
  }
}
