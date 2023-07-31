package org.example.demo

import org.apache.spark.sql.SparkSession
import org.example.utils.InitUtils

/**
 * @author: create by spz
 * @version: V1.0
 * @description: 粗糙的实现一下缺失字段补全功能
 * @date: 2023/7/27
 */
object SQlParserDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = InitUtils.createSparkSessionLocal()

    // 目标表
    val odsDf = parseCol(sparkSession,"input/total.csv")
    //    odsDf.show(false)
    val odsDfCol = odsDf.count()
    println("目标表列数量： " + odsDf.count())

    // 历史表
    val stgDf = parseCol(sparkSession,"input/blacklist.csv")
    //    stgDf.show(false)
    val stgDfCol = stgDf.count()
    println("历史表列数量： " + stgDf.count())


    // 如果数据量不一致，则进行关联开始字段补全
    if(odsDfCol != stgDfCol){
      // 目标表和在线表join
      val resultDf = odsDf.join(stgDf,odsDf.col("col") === stgDf.col("col"),"outer")
        .toDF("result_col","his_col")
      println("历史表缺失列数量： " + resultDf.filter("his_col is null").count())

      // 打印缺失字段
      resultDf.filter("his_col is null").show(50,false)

    }else{
      println("两张表字段一致，无需补全")
    }

    sparkSession.stop()
  }

  // 读取csv文件解析表字段
  def parseCol(sparkSession:SparkSession,path:String) = {
    val df = sparkSession.read.csv(path).toDF("col")
    df
  }

}
