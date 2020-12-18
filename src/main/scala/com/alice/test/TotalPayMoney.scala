package com.alice.test

import com.alice.base.BaseModel
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/6/14 09:02
 * @Description: 
       基于用户的总消费金额的统计型标签开发
 */
object TotalPayMoney extends BaseModel{

  override def setAppName(): String = "TotalPayTag"

  override def setFourTagId(): String = "148"

  /**
    * 重写Hbase与五级标签数据处理合并的步骤
    * @param spark
    * @param fiveTagDF
    * @param hbaseDF
    * @return
    */
  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    // 引入隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 对5级标签的数据进行处理
    val fiveTagDFS: DataFrame = fiveTagDF.map(row => {
      // row 是一条数据
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString

      var start: String = ""
      var end: String = ""

      val arr: Array[String] = rule.split("-")

      if (arr != null && arr.length == 2) {
        start = arr(0)
        end = arr(1)
      }

      // 封装
      (id, start, end)
    }).toDF("id", "start", "end")


    // 对Hbase的数据进行处理计算
    val TotalPayData: DataFrame = hbaseDF.groupBy("memberId").agg(sum("paidAmount").cast("Int").as("sumAmount"))

    TotalPayData.show(5)

    val dataJoin: DataFrame = TotalPayData.join(fiveTagDFS,TotalPayData.col("sumAmount").between(fiveTagDFS.col("start"),fiveTagDFS.col("end")))

    dataJoin.show()
    println("-----------------------------------------------------")

    // 选出我们最终需要的字段，返回需要和 HBase 中旧数据合并的新数据
    val SumTransactionNewTags: DataFrame = dataJoin.select('memberId.as("userId"),'id.as("tagsId"))

    SumTransactionNewTags

  }

  def main(args: Array[String]): Unit = {

     exec()

  }
  }
