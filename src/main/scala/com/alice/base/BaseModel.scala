package com.alice.base

import java.util.Properties

import com.alice.bean.HBaseMeta
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/6/13 08:49
 * @Description: 

    此代码用户编写用户画像项目可以重用的代码
 */
trait BaseModel {

  // 所有重复的代码(功能)都抽取到这里
  // 设置任务的名称
  def setAppName():String

  // 设置四级标签id
  def setFourTagId():String

  /* 1. 初始化SparkSession对象  */
  private val spark:SparkSession = SparkSession.builder().appName(setAppName()).master("local[*]").getOrCreate()

  //导入隐式转换
  import org.apache.spark.sql.functions._
  import spark.implicits._

  /* 2. 连接MySQL  */
  // 读取application.conf 内的配置
  private val config: Config = ConfigFactory.load()
  // 获取url
  private val url : String = config.getString("jdbc.mysql.url")
  // 获取tableName
  private val tableName : String = config.getString("jdbc.mysql.tablename")


  def getMySQLDF: DataFrame = {
    // 连接MySQL数据库
    spark.read.jdbc(url,tableName,new Properties)
  }


  /* 3. 读取MySQL数据库的四级标签  */

  def getFourTag (mysqlCoon: DataFrame): HBaseMeta ={
    //读取HBase中的四级标签
    val fourTagsDS: Dataset[Row] = mysqlCoon.select("id","rule").where("id="+setFourTagId)
    //切分rule
    val KVMap: Map[String, String] = fourTagsDS.map(row => {
      // 获取到rule值
      val RuleValue: String = row.getAs("rule").toString
      // 使用“##”对数据进行切分
      val KVMaps: Array[(String, String)] = RuleValue.split("##").map(kv => {
        val arr: Array[String] = kv.split("=")
        (arr(0), arr(1))
      })
      KVMaps
    }).collectAsList().get(0).toMap       //封装成map
    //   将Map 转换成HBaseMeta样例类
    val hbaseMeta: HBaseMeta = toHBaseMeta(KVMap)
    hbaseMeta
  }

  /* 4. 读取五级标签数据【单独处理】*/
  def getFiveTagDF(mysqlConn:DataFrame): DataFrame ={

    mysqlConn.select("id","rule").where("pid="+setFourTagId).toDF()
  }

  /* 5. 读取hbase中的数据，这里将hbase作为数据源进行读取 */
  def getHbase(hbaseMeta: HBaseMeta): DataFrame ={
    val hbaseDatas: DataFrame = spark.read.format("com.alice.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts",hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()

       hbaseDatas
  }

  /* 6. 五级数据与 HBase 数据进行打标签【单独处理】 */
  def getNewTag(spark: SparkSession,fiveTagDF:DataFrame,hbaseDF:DataFrame):DataFrame



  /**
    * 7.合并历史数据
    * 将标签写入HBase
    *
    * @param newTags 新标签
    * @return 返回最终标签
    */
  def joinAllTags(newTags: DataFrame): DataFrame = {
    //读取HBase 中的历史数据
    val oldTags: DataFrame = spark.read.format("com.alice.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, "192.168.10.20")
      .option(HBaseMeta.ZKPORT, "2181")
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()

    //使用join将新数据和旧数据的tagsId合并到一起
    val allTags: DataFrame = oldTags.join(newTags, oldTags("userId") === newTags("userId"))


    //  创建一个新的udf函数,用来拼接 tagsId
    val getAllTags: UserDefinedFunction = udf((oldTagsId: String, newTagsId: String) => {
      if (oldTagsId == "" && newTagsId != "") {
        newTagsId
      } else if (oldTagsId != "" && newTagsId == "") {
        oldTagsId
      } else if (oldTagsId == "" && newTagsId == "") {
        ""
      } else {
        val str: String = oldTagsId + "," + newTagsId
        str.split(",").distinct.mkString(",")
      }
    })

    //获取最终结果
    allTags.select(
      when(oldTags("userId").isNotNull, oldTags("userId"))
        .when(newTags("userId").isNotNull, newTags("userId"))
        .as("userId"),
      getAllTags(oldTags("tagsId"), newTags("tagsId"))
        .as("tagsId")

    )

  }


  /**
    * 8. 新建一个方法，用于保存数据  save
    * @param allTags   最终的结果
    */
  def save(allTags: DataFrame): Unit = {
    //把最终结果保存到HBase
    allTags.write.format("com.alice.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, "192.168.10.20")
      .option(HBaseMeta.ZKPORT, "2181")
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .save()

    println("结果保存完毕!!!")

  }


  /* 9. 断开连接 */
  def close(): Unit = {
    spark.close()
  }

  //将mysql中的四级标签的rule  封装成HBaseMeta
  //方便后续使用的时候方便调用
  def toHBaseMeta(KVMap: Map[String, String]): HBaseMeta = {
    //开始封装
    HBaseMeta(KVMap.getOrElse("inType",""),
      KVMap.getOrElse(HBaseMeta.ZKHOSTS,""),
      KVMap.getOrElse(HBaseMeta.ZKPORT,""),
      KVMap.getOrElse(HBaseMeta.HBASETABLE,""),
      KVMap.getOrElse(HBaseMeta.FAMILY,""),
      KVMap.getOrElse(HBaseMeta.SELECTFIELDS,""),
      KVMap.getOrElse(HBaseMeta.ROWKEY,"")
    )
  }


  /**
    * 按照先后顺序, 连接mysql数据库, 读取四级,五级HBase数据
    * 打标签,最终写入
    */
  def exec(): Unit = {
    //1.设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    //2.连接mysql
    val mysqlConnection: DataFrame = getMySQLDF
    //3. 读取mysql数据库中的四级标签
    val fourTags: HBaseMeta = getFourTag(mysqlConnection)
    //4. 读取mysql数据库中的五级标签
    val fiveTags: Dataset[Row] = getFiveTagDF(mysqlConnection)
    //读取HBase 中的数据
    val hBaseMea: DataFrame = getHbase(fourTags)

    //读取新获取的数据
    val newTags: DataFrame = getNewTag(spark,fiveTags, hBaseMea)
    //newTags.show()

    //获取最终结果
    val allTags: DataFrame = joinAllTags(newTags)
    //allTags.show()

    //保存到HBase
    save(allTags)
    //断开连接
    close()

  }


}
