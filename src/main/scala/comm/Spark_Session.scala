package comm

import org.apache.spark.sql.SparkSession

class Spark_Session {//集群配置信息
  val warehouseLocation: String = System.getProperty("user.dir") + "spark-warehouse" // default location for managed databases and tables
  val spark: SparkSession = SparkSession.
    builder().
    config("spark.sql.warehouse.dir", warehouseLocation).
    enableHiveSupport().
    config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    config("spark.sql.codegen",value = true).
    config("spark.sql.unsafe.enabled",value = true).
    config("spark.shuffle.manager","tungsten-sort").
    config("spark.memory.storageFraction",0.4).
    config("spark.shuffle.service.enable", value = true).
    config("spark.default.parallelizm",500).
    config("spark.sql.shuffle.partitions",500).
    config("spark.sql.auto.repartition", value = true).
    config("spark.debug.maxToStringFields",300).
    config("spark.sql.inMemoryColumnStorage.compressed", value = true).
    config("es.http.timeout","100m").
    config("es.index.auto.create", "true").
    config("es.nodes", "192.168.50.47").
    config("es.port","9200").
    config("spark.yarn.executor.memoryOverhead","2048m").
    config("spark.sql.codegen.wholeStage",value = false).
    config("spark.locality.wait", 0).
    getOrCreate()
}
//
object Spark_Session {
  val spark_session = new Spark_Session
  val spark: SparkSession = spark_session.spark
  spark.sparkContext.setLogLevel("ERROR")

}

