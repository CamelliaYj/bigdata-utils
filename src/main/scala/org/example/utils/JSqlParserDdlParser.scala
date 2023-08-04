package org.example.utils

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.create.table.{ColumnDefinition, CreateTable}
import org.apache.spark.sql.Column

import java.io.{File, FilenameFilter, IOException}
import java.util
import java.util.ArrayList
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * @author: create by spz
 * @version: V1.0
 * @description: 根据DDL生成旧表缺失字段
 * @date: 2023/7/27
 */
object JSqlParserDdlParser {
  def main(args: Array[String]): Unit = {

    val sparkSession = InitUtils.createSparkSessionLocal()

    // 获取目标表文件路径
    val filesNew: List[String] = listFiles(new File("input/ods"))
    // 解析目标表ddl获取字段名和字段类型
    val fileNamesNew = extractFieldNamesAndTypesFromDDL(readFile(filesNew(0)))
    println(fileNamesNew)
    println(fileNamesNew.size)

    // 1.构建目标表字段DataFrame
    import sparkSession.implicits._
    val newDF = sparkSession.sparkContext.makeRDD(fileNamesNew)
      .toDF("new_col_name","new_col_type")
      .selectExpr("replace(new_col_name,'`','') as new_col_name","new_col_type")

    // 获取历史表文件路径
    val filesHis: List[String] = listFiles(new File("input/his"))

    // 遍历历史表文件，构建历史表字段DataFrame
    for (file <- filesHis){
      // 解析历史表ddl获取字段名和字段类型
      val fileNamesHis = extractFieldNamesAndTypesFromDDL(readFile(file))
      println("当前历史表：" + file.substring(file.lastIndexOf("/") + 1))
      println("历史表字段列表：" + fileNamesHis)
      println("历史表字段数量：" + fileNamesHis.size)

      // 如果目标表字段数量大于历史表字段数量，则进行关联开始历史表字段补全
      if(fileNamesNew.size != fileNamesHis.size){
        // 2.构建历史表字段DataFrame
        val hisDF = sparkSession.sparkContext.makeRDD(fileNamesHis)
          .toDF("his_col_name","his_col_type")
          .selectExpr("replace(his_col_name,'`','') as his_col_name","his_col_type")
        // 3.关联
        val joinDF = newDF.join(hisDF, newDF("new_col_name") === hisDF("his_col_name") , "full")
        // 4.缺失字段
        joinDF.filter("his_col_name is null or new_col_name is null").selectExpr(
          "if(his_col_name is null,new_col_name,his_col_name) as miss_col_name",
          "if(his_col_name is null, lower(new_col_type), his_col_type) as miss_col_type")
          .selectExpr("miss_col_name","miss_col_type","if(miss_col_type in ('string','varchar'),'\\'\\'','null') as miss_col_value")
          .selectExpr("miss_col_name","miss_col_type",
            "concat_ws(' as ',miss_col_value,miss_col_name) as miss_col_ddl")
          .show(false)
      }else{
        println("无字段缺失")
      }
    }


    sparkSession.stop()

  }

  /***
   * 解析表ddl获取字段名
   * @param ddl
   * @return
   */
  def extractFieldNamesFromDDL(ddl:String) = {
    var list = ListBuffer[String]()
    try {
      val statement: Statement = CCJSqlParserUtil.parse(ddl);

      val createTable: CreateTable = statement.asInstanceOf[CreateTable];
      val columnDefinitions: util.List[ColumnDefinition] = createTable.getColumnDefinitions();

      for (i <- 0 until columnDefinitions.size()) {
        val columnDefinition: ColumnDefinition = columnDefinitions.get(i);
        val columnName: String = columnDefinition.getColumnName();
        list :+= columnName
      }

    } catch {
      case e: Exception => {
        e.printStackTrace();
      }
    }

    list
  }

  /***
   * 解析表ddl获取字段名和字段类型
   * @param ddl
   * @return
   */
  def extractFieldNamesAndTypesFromDDL(ddl:String) = {
    var list = ListBuffer[(String,String)]()
    try {
      val statement: Statement = CCJSqlParserUtil.parse(ddl);

      val createTable: CreateTable = statement.asInstanceOf[CreateTable];
      val columnDefinitions: util.List[ColumnDefinition] = createTable.getColumnDefinitions();

      for (i <- 0 until columnDefinitions.size()) {
        val columnDefinition: ColumnDefinition = columnDefinitions.get(i);
        val columnName: String = columnDefinition.getColumnName();
        val dataType = columnDefinition.getColDataType.getDataType
        list :+= (columnName,dataType)
      }

    } catch {
      case e: Exception => {
        e.printStackTrace();
      }
    }

    list
  }


  /***
   * 解析表ddl获取字段类型
   * @param ddl
   * @return
   */
  def extractFieldTypesFromDDL(ddl:String) = {
    val list = new ArrayList[String]()

    try {
      val statement: Statement = CCJSqlParserUtil.parse(ddl);

      val createTable: CreateTable = statement.asInstanceOf[CreateTable];
      val columnDefinitions: util.List[ColumnDefinition] = createTable.getColumnDefinitions();

      for (i <- 0 until columnDefinitions.size()) {
        val columnDefinition: ColumnDefinition = columnDefinitions.get(i);
        val dataType = columnDefinition.getColDataType.getDataType
        list.add(dataType)
      }

    } catch {
      case e: Exception => {
        e.printStackTrace();
      }
    }

    list
  }

  /**
   * 读取外部文件
   * @param filePath
   * @return
   * @throws IOException
   */
  def readFile(filePath:String):String = {
    val file = Source.fromFile(filePath)
    val content = file.mkString
    file.close()
    content
  }

  /**
   * 遍历目录
   * @param directory
   */
  def listFiles(directory:File ):List[String] = {
    val files: Array[File] = directory.listFiles()
    val result = ListBuffer[String]()

    if (files != null) {
      for (file: File <- files) {
        if (file.isDirectory) {
          result ++= listFiles(file) //递归调用
        } else {
          if(file.getName.endsWith(".sql")){
            result += file.getPath //添加文件路径
          }
        }
      }
    }
    result.toList
  }

}
