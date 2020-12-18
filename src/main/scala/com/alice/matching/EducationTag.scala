package com.alice.matching

import java.util.Properties
import com.alice.bean.{HBaseMeta, TagRule}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/6/8 16:51
 * @Description: 
    学历标签开发
 */
object EducationTag {

  // 程序入口
  def main(args: Array[String]): Unit = {

    // 创建SparkSQL
    // 用于读取mysql，hbase等数据
    val spark: SparkSession = SparkSession.builder().appName("EducationTag").master("local[*]").getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 设置Spark连接MySQL所需要的字段
    var url: String ="jdbc:mysql://bd001:3306/tags_new2?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    var table: String ="tbl_basic_tag"   //mysql数据表的表名
    var properties:Properties = new Properties

    // 连接MySQL
    val mysqlConn: DataFrame = spark.read.jdbc(url,table,properties)

    // 导入隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    import scala.collection.JavaConverters._
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 3.读取MySQL数据库的四级标签
    val fourTagsDS: Dataset[Row] = mysqlConn.select("id","rule").where("id=72")

    val KVMaps: Map[String, String] = fourTagsDS.map(row => {
      // 获取到rule值
      val RuleValue: String = row.getAs("rule").toString

      // 使用“##”对数据进行切分
      val KVMaps: Array[(String, String)] = RuleValue.split("##").map(kv => {
        val arr: Array[String] = kv.split("=")
        (arr(0), arr(1))
      })
      KVMaps
    }).collectAsList().get(0).toMap

    println(KVMaps)

    //Map(selectFields -> id,education, inType -> HBase, zkHosts -> 192.168.10.20, zkPort -> 2181, hbaseTable -> tbl_users, family -> detail)

    val hbaseMeta: HBaseMeta = toHBaseMeta(KVMaps)

    // 4. 读取mysql数据库的五级标签
    // 匹配职业
    val fiveTagsDS: Dataset[Row] = mysqlConn.select("id","rule").where("pid=72")

    // 将FiveTagsDS 封装成样例类 TagRule
    val fiveTageList: List[TagRule] = fiveTagsDS.map(row => {
      // row 是一条数据
      // 获取出id 和 rule
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString.toString
    
      // 封装样例类
      TagRule(id, rule)
    }).collectAsList()
      .asScala.toList   // 将DataSet => list 集合

    for(a<- fiveTageList){
      println(a.id+"      "+a.rule)
    }
    //73      1
    //74      2
    //75      3
    //76      4
    //77      5
    //78      6
    //79      7

    // 读取hbase中的数据，这里将hbase作为数据源进行读取
    val hbaseDatas: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts",hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()

    // 展示一些数据
    hbaseDatas.show(5)

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

    // 6. 使用五级标签与HBase的数据进行匹配获取标签
    val EduNewTags : DataFrame = hbaseDatas.select('id.as ("userId"),getUserTags('education).as("tagsId"))
    EduNewTags.show(5)


    /**  定义一个udf，用于处理将旧数据和新数据做一个合并  **/
    val getAllTags: UserDefinedFunction = udf((OldDatas: String, EduNewTags: String) => {

      if (OldDatas == "") {
        EduNewTags
      } else if (EduNewTags == "") {
        OldDatas
      } else if (OldDatas == "" && EduNewTags == "") {
        ""
      } else {
        val allTags: String = OldDatas + "," + EduNewTags
        // 可能会出现重复打标签的情况
        // 这里需要对数据进行去重
        allTags.split(",").distinct
          // 使用逗号进行分割，返回字符串类型
          .mkString(",")
      }
    })

    // 7、解决数据覆盖的问题
    // 读取test，追加标签后覆盖写入
    // 标签去重

    val OldDatas: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts","192.168.10.20")
      .option(HBaseMeta.ZKPORT, "2181")
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()

     //    展示旧数据
     //    genderOldDatas.show(5)

     // 新表和旧表进行join
     val joinTags: DataFrame = OldDatas.join(EduNewTags, OldDatas("userId") === EduNewTags("userId"))

     // 查询数据
    val allTags: DataFrame = joinTags.select(
      // 处理第一个字段
      when((OldDatas.col("userId").isNotNull), (OldDatas.col("userId")))
        .when((EduNewTags.col("userId").isNotNull), (EduNewTags.col("userId")))
        .as("userId"),

      getAllTags(OldDatas.col("tagsId"), EduNewTags.col("tagsId")).as("tagsId")

    )

    // 展示合并的最终结果
    allTags.show()

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
