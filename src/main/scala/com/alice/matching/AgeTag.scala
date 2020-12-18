package com.alice.matching

import java.util.Properties

import com.alice.bean.HBaseMeta
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/6/11 17:05
 * @Description:

    基于用户年龄的统计型标签开发
 */
object AgeTag {

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
    import spark.implicits._

    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 读取MySQL数据库的四级标签
    val fourTagsDS: Dataset[Row] = mysqlConn.select("rule").where("id=90")

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
    //Map(selectFields -> id,birthday, inType -> HBase, zkHosts -> 192.168.10.20, zkPort -> 2181, hbaseTable -> tbl_users, family -> detail)

    // 将Map 转换成HBaseMeta的样例类
    val hbaseMeta: HBaseMeta = toHBaseMeta(KVMap)

    //4. 读取mysql数据库的五级标签
    val fiveTagsDS: Dataset[Row] = mysqlConn.select("id","rule").where("pid=90")

    val fiveTagDF: DataFrame = fiveTagsDS.map(row => {
      // row 是一条数据
      // 获取出id 和 rule
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString

      /*
          1     1992-05-31
         10     1980-10-13
       */
      // 因为在数据库中，标签值为“其他”的 rule 值 为 空
      // 所以我们这里需要进行一个判空

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
   //+---+--------+--------+
    //| id|   start|     end|
    //+---+--------+--------+
    //| 91|19700101|19791231|
    //| 92|19800101|19891231|
    //| 93|19900101|19991231|
    //| 94|20000101|20091231|
    //| 95|        |        |
    //+---+--------+--------+


    // 5. 读取hbase中的数据，这里将hbase作为数据源进行读取
    val hbaseDatas: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts",hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()



    // 6.数据汇总
    /*
         hbase数据中的birthday格式是 1970-01-01
         但是五级标签中日期格式是   19700108
         所以，需要将yyyy-MM-dd  的数据转换为 yyyyMMdd
     */

    val hbaseDF: DataFrame = hbaseDatas.select(
      'id.as("userId"),
      regexp_replace('birthday, "-", "").as("birthday")
    )
    hbaseDF.show(3)

    // 展示一些数据
    //+------+--------+
    //|userId|  birthday|
    //+------+--------+
    //|     1|19920531|
    //|    10|19801013|
    //|   100|19931028|

    // join
    val joinDF: DataFrame = hbaseDF.join(fiveTagDF,hbaseDF.col("birthday").between(fiveTagDF.col("start"),fiveTagDF.col("end")))

    joinDF.show(5)
    //+------+--------+---+--------+--------+
    //|userId|birthday| id|   start|     end|
    //+------+--------+---+--------+--------+
    //|     1|19920531| 93|19900101|19991231|
    //|    10|19801013| 92|19800101|19891231|
    //|   100|19931028| 93|19900101|19991231|
    //|   101|19960818| 93|19900101|19991231|
    //|   102|19960728| 93|19900101|19991231|
    //+------+--------+---+--------+--------+

    val jobNewTags: DataFrame = joinDF.select('userId.as("userId"),'id.as("tagsId"))

    jobNewTags .show(5)
    //+------+------+
    //|userId|tagsId|
    //+------+------+
    //|     1|    93|
    //|    10|    92|
    //|   100|    93|
    //|   101|    93|
    //|   102|    93|
    //+------+------+


    /*  定义一个udf,用于处理旧数据和新数据中的数据 */
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

    // 7、解决数据覆盖的问题
    // 读取test，追加标签后覆盖写入
    // 标签去重

    val genderOldDatas: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts","192.168.10.20")
      .option(HBaseMeta.ZKPORT, "2181")
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()

    //hbase中的历史数据
    genderOldDatas.show(5)


    // 新表和旧表进行join
    val joinTags: DataFrame = genderOldDatas.join(jobNewTags, genderOldDatas("userId") === jobNewTags("userId"))

    val allTags: DataFrame = joinTags.select(
      // 处理第一个字段
      when(genderOldDatas.col("userId").isNotNull, genderOldDatas.col("userId"))
        .when(jobNewTags.col("userId").isNotNull, jobNewTags.col("userId"))
        .as("userId"),

      getAllTages(genderOldDatas.col("tagsId"), jobNewTags.col("tagsId")).as("tagsId")
    )

    // 新数据与旧数据汇总之后的数据
    allTags.show(5)

//    +------+-------------+
//    |userId|       tagsId|
//    +------+-------------+
//    |   296|5,71,81,87,92|
//    |   467|6,71,81,87,91|
//    |   675|6,68,81,87,93|
//    |   691|5,66,81,89,92|
//    |   829|5,70,81,89,91|

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
