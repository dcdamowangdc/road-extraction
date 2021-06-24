package comm

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class Spark_Session {//集群配置信息
  val conf = new SparkConf().setMaster("local[*]")
  val spark: SparkSession = SparkSession.
    builder().
    config(conf).
    getOrCreate()
}
//
object Spark_Session {
  val spark_session = new Spark_Session
  val spark: SparkSession = spark_session.spark
  spark.sparkContext.setLogLevel("ERROR")

}

