package com.alice.test

import com.alice.base.BaseModel
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/6/14 09:32
 * @Description: 
    
 */
object TotalPayCount extends BaseModel{
  override def setAppName(): String = "TotalPayCount"

  override def setFourTagId(): String = "154"

  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    // 引入隐式转换
    import spark.implicits._
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    val fiveTagDFS: DataFrame = fiveTagDF.map(row => {
      // row 是一条数据
      // 获取出id 和 rule
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


    // 对HBase中的数据进行处理
    val TotalCountData: DataFrame = hbaseDF.groupBy("memberId").agg(count("orderAmount").as("countAmount"))

    TotalCountData.show()

    val dataJoin: DataFrame = TotalCountData.join(fiveTagDFS, TotalCountData.col("countAmount")
      .between(fiveTagDFS.col("start"), fiveTagDFS.col("end")))

    dataJoin.show()

    // 选出最终需要的字段
    val TotalPayNewTags: DataFrame = dataJoin.select('memberId.as("userId"),'id.as("tagsId"))

    TotalPayNewTags

  }

  def main(args: Array[String]): Unit = {


    exec()


  }
}
