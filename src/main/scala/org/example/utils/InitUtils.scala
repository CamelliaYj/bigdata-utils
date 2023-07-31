package org.example.utils

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/**
 * @author: create by spz
 * @version: V1.0
 * @description: SparkSession 初始化
 * @date: 2023/7/27
 */
object InitUtils {

  /**
   * create SparkSession
   * @return
   */
  def createSparkSession(): SparkSession={
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.crossJoin.enabled","true")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    spark
  }

  /**
   * 创建spark上下文环境
   * @param appName
   * @return
   */
  def createSparkSession(appName:String): SparkSession={
    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.crossJoin.enabled","true")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    spark
  }

  def createSparkSessionLocal(): SparkSession={
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    spark
  }
}
