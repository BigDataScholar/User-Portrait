package com.alice.test

import com.alice.base.BaseModel
import com.alice.bean.TagRule
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

/*
 * @Author: Alice菌
 * @Date: 2020/6/13 09:48
 * @Description:

     基于用户的Job标签，做测试使用
 */
object Test extends BaseModel{

  override def setAppName(): String = "Job"

  override def setFourTagId(): String = "65"

  // 重写Hbase数据与MySQL五级标签数据处理的方法
  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    // 引入隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    import scala.collection.JavaConverters._
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 对5级标签的数据进行处理
    val fiveTageList: List[TagRule] = fiveTagDF.map(row => {
      // row 是一条数据
      // 获取出id 和 rule
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString

      // 封装样例类
      TagRule(id,rule)
    }).collectAsList()   // 将DataSet转换成util.List[TagRule]   这个类型遍历时无法获取id,rule数据
      .asScala.toList    // 将util.List转换成list   需要隐式转换    import scala.collection.JavaConverters._

    // 需要自定义UDF函数
    val getUserTags: UserDefinedFunction = udf((rule: String) => {

      // 设置标签的默认值
      var tagId: Int = 0
      // 遍历每一个五级标签的rule
      for (tagRule <- fiveTageList) {

        if (tagRule.rule == rule) {
          tagId = tagRule.id
        }
      }
      tagId
    })


    val jobNewTags : DataFrame = hbaseDF.select('id.as ("userId"),getUserTags('job).as("tagsId"))
    jobNewTags.show(5)

    jobNewTags

  }
  def main(args: Array[String]): Unit = {

    exec()

  }

}
