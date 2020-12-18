package com.alice.statistics

import java.util.Properties

import com.alice.bean.HBaseMeta
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/6/12 18:30
 * @Description: 

      基于用户的单笔最高标签
 */
object HighestMoneyTag {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("AgeTag").master("local[*]").getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 设置Spark连接MySQL所需要的字段
    val url: String ="jdbc:mysql://bd001:3306/tags_new2?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val table: String ="tbl_basic_tag"   //mysql数据表的表名
    val properties:Properties = new Properties

    // 连接MySQL
    val mysqlConn: DataFrame = spark.read.jdbc(url,table,properties)

    // 引入隐式转换
    import  spark.implicits._

    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 读取MySQL数据库的四级标签
    val fourTagsDS: Dataset[Row] = mysqlConn.select("rule").where("id=132")

    val KVMap: Map[String, String] = fourTagsDS.map(row => {

      // 获取到rule值
      val RuleValue: String = row.getAs("rule").toString

      // 使用"##"对数据进行切分
      val KVMaps: Array[(String, String)] = RuleValue.split("##").map(kv => {

        val arr: Array[String] = kv.split("=")
        (arr(0), arr(1))

      })
      KVMaps
    }).collectAsList().get(0).toMap

    println(KVMap)


    // 将Map 转换成HBaseMeta的样例类
    val hbaseMeta: HBaseMeta = toHBaseMeta(KVMap)

    //4. 读取mysql数据库的五级标签
    val fiveTagsDS: Dataset[Row] = mysqlConn.select("id","rule").where("pid=132")

    val fiveTagDF: DataFrame = fiveTagsDS.map(row => {
      // row 是一条数据
      // 获取出id 和 rule
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString

      //133    1-999
      //134    1000-2999
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

    fiveTagDF.show()
   //+---+-----+----+
    //| id|start| end|
    //+---+-----+----+
    //|133|    1| 999|
    //|134| 1000|2999|
    //|135| 3000|4999|
    //|136| 5000|9999|
    //+---+-----+----+

    // 5. 读取hbase中的数据，这里将hbase作为数据源进行读取
    val hbaseDatas: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts",hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()

    hbaseDatas.show(5)
    //+--------+-----------+
    //|memberId|orderAmount|
    //+--------+-----------+
    //|13823431|    2479.45|
    //| 4035167|    2449.00|
    //| 4035291|    1099.42|
    //| 4035041|    1999.00|
    //|13823285|    2488.00|
    //+--------+-----------+

    // 因为一个用户可能会有多条数据 ，也就会有多个支付金额
    // 我们需要将数据按照用户id进行分组，然后求取金额数最大的作为用户的单笔最高数据
    val userMaxAmount: DataFrame = hbaseDatas.groupBy("memberId").agg(max("orderAmount").cast("Int").as("maxAmount"))

    userMaxAmount.show()
    //+---------+---------+
    //| memberId|maxAmount|
    //+---------+---------+
    //| 13822725|      999|
    //| 13823083|      999|
    //|138230919|      999|
    //| 13823681|      999|
    //|  4033473|     9999|

    // 将 Hbase的数据与 五级标签的数据进行 关联
    val dataJoin: DataFrame = userMaxAmount.join(fiveTagDF, userMaxAmount.col("maxAmount")
      .between(fiveTagDF.col("start"), fiveTagDF.col("end")))

    dataJoin.show()
    //+---------+---------+---+-----+----+
    //| memberId|maxAmount| id|start| end|
    //+---------+---------+---+-----+----+
    //| 13822725|      999|133|    1| 999|
    //| 13823083|      999|133|    1| 999|
    //|138230919|      999|133|    1| 999|
    //| 13823681|      999|133|    1| 999|
    //|  4033473|     9999|136| 5000|9999|

    println("---------------------------------------------")
    // 选出我们最终需要的字段，返回需要和Hbase中旧数据合并的新数据
    val ConsumptionNewTags: DataFrame = dataJoin.select('memberId.as("userId"),'id.as("tagsId"))

    ConsumptionNewTags.show(5)
    //+---------+------+
    //|   userId|tagsId|
    //+---------+------+
    //| 13822725|   133|
    //| 13823083|   133|
    //|138230919|   133|
    //| 13823681|   133|
    //|  4033473|   136|
    //+---------+------+


    // 7、解决数据覆盖的问题
    // 读取test，追加标签后覆盖写入
    // 标签去重
    /*  定义一个udf,用于处理旧数据和新数据中的数据合并的问题 */
    val getAllTages: UserDefinedFunction = udf((genderOldDatas: String, jobNewTags: String) => {

      if (genderOldDatas == "") {
        jobNewTags
      } else if (jobNewTags == "") {
        genderOldDatas
      } else if (genderOldDatas == "" && jobNewTags == "") {
        ""
      } else {
        val alltages: String = genderOldDatas + "," + jobNewTags  //可能会出现 83,94,94
        // 对重复数据去重
        alltages.split(",").distinct // 83 94
          // 使用逗号分隔，返回字符串类型
          .mkString(",") // 83,84
      }
    })

    // 读取hbase中的历史数据
    val genderOldDatas: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts","192.168.10.20")
      .option(HBaseMeta.ZKPORT, "2181")
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()

    // 新表和旧表进行join
    val joinTags: DataFrame = genderOldDatas.join(ConsumptionNewTags, genderOldDatas("userId") === ConsumptionNewTags("userId"))

    joinTags.show()


    val allTags: DataFrame = joinTags.select(
      // 处理第一个字段
      when((genderOldDatas.col("userId").isNotNull), (genderOldDatas.col("userId")))
        .when((ConsumptionNewTags.col("userId").isNotNull), (ConsumptionNewTags.col("userId")))
        .as("userId"),
      getAllTages(genderOldDatas.col("tagsId"), ConsumptionNewTags.col("tagsId")).as("tagsId")
    )


    // 新数据与旧数据汇总之后的数据
    allTags.show(100)

    // 将最终结果进行覆盖
    allTags.write.format("com.czxy.tools.HBaseDataSource")
      .option("zkHosts", hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE,"test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .option("repartition",1)
      .save()
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
}
