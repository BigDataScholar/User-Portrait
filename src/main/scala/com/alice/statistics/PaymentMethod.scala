package com.alice.statistics

import java.util.Properties

import com.alice.bean.HBaseMeta
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/6/12 11:46
 * @Description: 

      基于用户的支付方式的统计型标签进行开发
 */
object PaymentMethod {
  def main(args: Array[String]): Unit = {

    // 1. 创建sparkSQL实例用于读取hbase，mysql中的数据
    val spark: SparkSession = SparkSession.builder().appName("PaymentMethod").master("local[*]").getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 2. 连接MySQL数据库
    val url: String ="jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val table: String ="tbl_basic_tag"
    val properties: Properties =new Properties

    val mysqlConn: DataFrame = spark.read.jdbc(url,table,properties)
    // 隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 3. 读取四级标签数据
    val fourDS: Dataset[Row] = mysqlConn.select("rule").where("id=106")

    val fourMap: Map[String, String] = fourDS.map(row => {
      // 使用 ##  切分再使用 = 切分
      row.getAs("rule").toString.split("##")
        .map(kv => {
          val arr: Array[String] = kv.split("=")
          (arr(0), arr(1))
        })
    }).collectAsList().get(0).toMap   // 将四级标签包含的hbase配置数据转换成Map存放

    // 将Map 转换成 HbaseMeta样例类
    val hbaseMeta: HBaseMeta = toHBaseMeta(fourMap)

    // 4. 读取五级标签数据
    val fiveDS: Dataset[Row] = mysqlConn.select("id","rule").where("pid=106")

    val fiveMap: Map[String, Int] = fiveDS.map(row => {
      // 获取数据
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString

      // 封装
      (rule, id)
    }).collect().toMap

    // 5. 读取hbase数据
    // 查出来的hbase数据中包含一个用户的多条支付数据
    // 我们需要将数据按照用户id进行分组，然后统计每种支付方式的个数，求最大值，就是用户用户最常用的支付方式
    val hbaseDatas: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      // hbaseMeta.zkHosts 就是 192.168.10.20  和 下面是两种不同的写法
      .option("zkHosts",hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()

    hbaseDatas.show()
    //+---------+-----------+
    //| memberId|paymentCode|
    //+---------+-----------+
    //| 13823431|     alipay|
    //|  4035167|     alipay|
    //|  4035291|     alipay|
    //|  4035041|     alipay|
/*    hbaseDatas.groupBy("memberId","paymentCode")
      .agg(count("paymentCode").as("count"))
      .withColumn("rn",rank().over(Window.partitionBy("memberId").orderBy('counts desc)))*/

    val userPaymentMethod: Dataset[Row] = hbaseDatas.groupBy("memberId", "paymentCode") //对数据进行分组  分组字段memberId和paymentCode
      .agg(count("paymentCode").as("counts")) //对分组后的数据求去总和  count(paymentCode)
      //+--------+-----------+------+
      //|memberId|paymentCode|counts|
      //+--------+-----------+------+
      //|13823481|     alipay|    96|
      //| 4035297|     alipay|    80|
      //|13823317|     kjtpay|    11|
      //|13822857|     alipay|   100|
      //| 4034541|     alipay|    96|

      .withColumn("rn", rank().over(Window.partitionBy("memberId").orderBy('counts desc)))
      // +---------+---------------+------+---+
      // memberId|    paymentCode|counts| rn|
      // +---------+---------------+------+---+
      // 13822725|         alipay|    89|  1|
      // 13822725|            cod|    12|  2|
      // 13822725|         kjtpay|     9|  3|
      // 13822725|          wspay|     3|  4|
      // 13822725|       giftCard|     2|  5|
      // 13822725|        prepaid|     1|  6|
      // 13823083|         alipay|    94|  1|
      // 13823083|            cod|    18|  2|
      // 13823083|         kjtpay|    12|  3|
      // 分组 top1
      .where("rn=1")

    userPaymentMethod.show(10)
    //+---------+-----------+------+---+
    //| memberId|paymentCode|counts| rn|
    //+---------+-----------+------+---+
    //| 13822725|     alipay|    89|  1|
    // 13822725|         cod|    89|  1|
    //| 13823083|     alipay|    94|  1|
    //|138230919|     alipay|    98|  1|
    //| 13823681|     alipay|    87|  1|
    //|  4033473|     alipay|   113|  1|

    //编写UDF,用于五级标签数据和Hbase数据合并时使用
    var  getTagId: UserDefinedFunction =udf((paymentCode:String)=>{
      //到五级标签获取数据
      var tagId: Option[Int] = fiveMap.get(paymentCode)

      //若没有获取到数据，就获取others
      if(tagId==null){
        tagId = fiveMap.get("others")
      }
      tagId
    })


    //用户的常用支付方式标签
    val userPaymentMethodTag: DataFrame = userPaymentMethod.select('memberId .as("userId"),getTagId('paymentCode).as("tagsId"))
    //    +---------+------+
    //    |   userId|tagsId|
    //    +---------+------+
    //    | 13822725|   133|
    //    | 13823083|   133|
    //    |138230919|   133|

    //与历史数据进行整合
    //通过自定义函数返回支付方式的id
    var getAllTagas: UserDefinedFunction = udf((oldTagsId:String, newTagsId:String)=>{
      if (oldTagsId==""){
        newTagsId
      }else if (newTagsId==""){
        oldTagsId
      }else if(oldTagsId==""&& newTagsId==""){
        ""
      }else{
        //拼接历史数据和新数据（可能有重复的数据）
        val alltags: String = oldTagsId+","+newTagsId   //83,94,94
        //对重复数据区中去重
        alltags.split(",").distinct//83  94
          //使用逗号分隔，返回字符串类型。
          .mkString(",")//83,94
      }
    })


    //7、解决数据覆盖的问题【新开发的标签会覆盖前面的所有标签】
    val oldTags: DataFrame = spark.read.format("com.czxy.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE,"test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()

    //追加新计算出来的标签到历史数据
    //新表join新表，条件是两个表的userId相等
    val joinTagas: DataFrame = oldTags.join(userPaymentMethodTag,oldTags("userId")===userPaymentMethodTag("userId"))

    joinTagas.show(5)
    //

    val allTags: DataFrame = joinTagas.select(
      //处理第一个字段    两个表中的多个userId字段，只读取一个
      when(oldTags.col("userId").isNotNull, oldTags.col("userId"))
        .when(userPaymentMethodTag.col("userId").isNotNull, userPaymentMethodTag.col("userId"))
        .as("userId"),

      //处理第二个字段  将两个字段个合并一起
      getAllTagas(oldTags.col("tagsId"),userPaymentMethodTag.col("tagsId")).as("tagsId")

    )

    allTags.show(5)
    //


    //8、将最终数据写入hbase
//    allTags.write.format("com.czxy.tools.HBaseDataSource")
//      .option("zkHosts", hbaseMeta.zkHosts)
//      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
//      .option(HBaseMeta.HBASETABLE,"test")
//      .option(HBaseMeta.FAMILY, "detail")
//      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
//      .save()

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
