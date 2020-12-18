package com.alice.test

import java.util.Date

import com.alice.base.BaseModel
import com.alice.bean.TagRule
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
/*
 * @Author: Alice菌
 * @Date: 2020/6/14 13:00
 * @Description: 

      基于用户的最后登录时间的统计型标签进行开发
 */
object LastLogin extends BaseModel{
  override def setAppName(): String = "LastLogin"

  override def setFourTagId(): String = "162"

  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    fiveTagDF.show()
    //+---+----+
    //| id|rule|
    //+---+----+
    //|163|   1|
    //|164|   7|
    //|165|  14|
    //|166|  30|
    //+---+----+

    // 引入隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    import scala.collection.JavaConverters._
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 对5级标签数据进行处理
    val fiveTagList: List[TagRule] = fiveTagDF.map(row => {

      // row是一条数据
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString

      TagRule(id, rule)

    }).collectAsList() // 将DataSet转换成util.List[TagRule]   这个类型遍历时无法获取id,rule数据
      .asScala.toList  // 将util.List转换成list   需要隐式转换    import scala.collection.JavaConverters._


    // 先获取到当前时间的时间戳
    val localTime: Long = new Date().getTime

    // 需要自定义UDF函数
    val getUserTags: UserDefinedFunction = udf((rule: Int) => {

      // 设置标签的默认值 【默认值为超过一个月，即id为167】
      var tagId: Int = 167

        // 先计算当前时间与传入时间的时间戳的差值
        val diffTime: Long = localTime-rule

        // 遍历每一个五级标签的rule
        for (tagRule <- fiveTagList) {

            // 与五级标签的所有时间进行对比，发现有小于某一个时间段的，就以其对应的id为返回值
            if (diffTime<Integer.parseInt(tagRule.rule)*1000*60*60){
              tagId = tagRule.id
            }
        }
      tagId
    })

      // 处理Hbase的数据
    val LastLoginNewTags: DataFrame = hbaseDF.select('id.as ("userId"),getUserTags('lastLoginTime).cast("Int").as("tagsId"))

    LastLoginNewTags

  }

  def main(args: Array[String]): Unit = {

    exec()

  }
}
