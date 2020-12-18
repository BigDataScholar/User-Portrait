package com.alice.statistics

import java.util.Properties

import com.alice.bean.HBaseMeta
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

/*
 * @Author: Alice菌
 * @Date: 2020/6/11 20:19
 * @Description: 

      基于用户消费周期的统计型标签进行开发

 */
object ConsumptionTag {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("ConsumptionTag").master("local[*]").getOrCreate()

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

    // 引入SparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 读取MySQL数据库的四级标签
    val fourTagsDS: Dataset[Row] = mysqlConn.select("rule").where("id=96")

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
    //Map(selectFields -> memberId,finishTime, inType -> HBase, zkHosts -> 192.168.10.20, zkPort -> 2181, hbaseTable -> tbl_orders, family -> detail)

    // 将Map 转换成 HBaseMeta 的样例类
    val hbaseMeta: HBaseMeta = toHBaseMeta(KVMap)

    // 4. 读取mysql数据库中的五级标签
    val fiveTagsDS: Dataset[Row] = mysqlConn.select("id","rule").where("pid=96")

    val fiveTagDF: DataFrame = fiveTagsDS.map(row => {
      // row 是一条数据
      // 获取出id 和 rule
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString

      // 因为在数据库中，标签值为 “其他”的 rule值为空
      // 所以这里我们需要进行一个判空

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

    //+---+-----+---+
    //| id|start|end|
    //+---+-----+---+
    //| 97|    1|  7|
    //| 98|    8| 14|
    //| 99|   15| 30|
    //|100|   31| 60|
    //|101|   61| 90|
    //|102|   91|120|
    //|103|  121|150|
    //|104|  151|180|
    //|105|     |   |
    //+---+-----+---+

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
    //+--------+----------+
    //|memberId|finishTime|
    //+--------+----------+
    //|13823431|1564415022|
    //| 4035167|1565687310|
    //| 4035291|1564681801|
    //| 4035041|1565799378|
    //|13823285|1565062072|
    //+--------+----------+

    val userFinishTime: Dataset[Row] = hbaseDatas.groupBy("memberId") // 对用户id进行分组
      .agg(max('finishTime).as("finishTime"))// 获取一个用户多个订单，finishTime最大的数据

    userFinishTime.show(5)

    //+---------+----------+
    //| memberId|finishTime|
    //+---------+----------+
    //| 13822725|1566056954|
    //| 13823083|1566048648|
    //|138230919|1566012606|
    //| 13823681|1566012541|
    //|  4033473|1566022264|
    //+---------+----------+


    // 计算当前时间与订单最后的时间差值    【为了方便查看数据，我们这里减去了280天，实际项目中不能这么做】
    val userDiffTime: DataFrame = userFinishTime.select('memberId.as("memberId"),(datediff(current_timestamp(), from_unixtime('finishTime)) - 280).as("finishTime"))

    userDiffTime.show(5)
    //+---------+----------+
    //| memberId|finishTime|
    //+---------+----------+
    //| 13822725|        20|
    //| 13823083|        20|
    //|138230919|        20|
    //| 13823681|        20|
    //|  4033473|        20|
    //+---------+----------+


    // 将用户的最后订单数据距离当前时间的差值数据，与五级标签数据进行关联
    val dataJoin: DataFrame = userDiffTime.join(fiveTagDF, userDiffTime.col("finishTime")
      .between(fiveTagDF.col("start"), fiveTagDF.col("end")))

    dataJoin.show(5)
    //+---------+----------+---+-----+---+
    //| memberId|finishTime| id|start|end|
    //+---------+----------+---+-----+---+
    //| 13822725|        20| 99|   15| 30|
    //| 13823083|        20| 99|   15| 30|
    //|138230919|        20| 99|   15| 30|
    //| 13823681|        20| 99|   15| 30|
    //|  4033473|        20| 99|   15| 30|
    //+---------+----------+---+-----+---+

    // 选出我们最终需要的字段，返回需要和Hbase中旧数据合并的新数据
    val ConsumptionNewTags: DataFrame = dataJoin.select('memberId.as("userId"),'id.as("tagsId"))

    ConsumptionNewTags.show(5)


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

    // 展示一些历史来瞅瞅
    genderOldDatas.show(5)

    //+------+-------------+
    //|userId|       tagsId|
    //+------+-------------+
    //|     1|6,68,81,87,93|
    //|    10|6,70,81,88,92|
    //|   100|6,68,81,88,93|
    //|   101|5,66,81,87,93|
    //|   102|6,66,81,89,93|
    //+------+-------------+

    // 新表和旧表进行join
    val joinTags: DataFrame = genderOldDatas.join(ConsumptionNewTags, genderOldDatas("userId") === ConsumptionNewTags("userId"))

    joinTags.show()

    println("- - - - - - - - - - - -")

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
