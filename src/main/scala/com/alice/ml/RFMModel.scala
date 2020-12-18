package com.alice.ml

import com.alice.base.BaseModel
import com.alice.bean.TagRule
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import scala.collection.immutable

/*
 * @Author: Alice菌
 * @Date: 2020/6/22 09:18
 * @Description: 

    此代码用于计算 用户画像价值模型

 */
object RFMModel extends BaseModel{

  // 设置任务名称
  override def setAppName(): String = "RFMModel"

  // 设置用户价值id
  override def setFourTagId(): String = "168"

  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    //fiveTagDF.show()
    /*
    +---+----+
    | id|rule|
    +---+----+
    |169|   0|
    |170|   1|
    |171|   2|
    |172|   3|
    |173|   4|
    |174|   5|
    |175|   6|
+---+----+
     */
    //hbaseDF.show()
    /*
    +---------+----------+--------------------+-----------+
    | memberId|finishTime|             orderSn|orderAmount|
    +---------+----------+--------------------+-----------+
    | 13823431|1564415022|gome_792756751164275|    2479.45|
    |  4035167|1565687310|jd_14090106121770839|    2449.00|
    |  4035291|1564681801|jd_14090112394810659|    1099.42|
    |  4035041|1565799378|amazon_7877495617...|    1999.00|
     */

    //RFM三个单词
    val recencyStr: String = "recency"
    val frequencyStr: String = "frequency"
    val monetaryStr: String = "monetary"

    // 特征单词
    val featureStr: String = "feature"
    val predictStr: String = "predict"

    // 计算业务数据
    // R(最后的交易时间到当前时间的距离)
    // F(交易数量【半年/一年/所有】)
    // M(交易总金额【半年/一年/所有】)

    // 引入隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    import scala.collection.JavaConverters._
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 用于计算 R 数值
    // 与当前时间的时间差 - 当前时间用于求订单中最大的时间
    val getRecency: Column = functions.datediff(current_timestamp(),from_unixtime(max("finishTime")))-300 as recencyStr

    // 计算F的值
    val getFrequency: Column = functions.count("orderSn") as frequencyStr

    // 计算M数值  sum
    val getMonetary: Column = functions.sum("orderAmount") as monetaryStr


    // 由于每个用户有多个订单，所以计算一个用户的RFM，需要使用用户id进行分组
    val getRFMDF: DataFrame = hbaseDF.groupBy("memberId")
      .agg(getRecency, getFrequency, getMonetary)

    getRFMDF.show(false)
    /*
    +---------+-------+---------+------------------+
    |memberId |recency|frequency|monetary          |
    +---------+-------+---------+------------------+
    |13822725 |10     |116      |179298.34         |
    |13823083 |10     |132      |233524.17         |
    |138230919|10     |125      |240061.56999999998|
     */

    //现有的RFM 量纲不统一，需要执行归一化   为RFM打分
    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分

    //计算R的分数
    var getRecencyScore: Column =functions.when((col(recencyStr)>=1)&&(col(recencyStr)<=3),5)
      .when((col(recencyStr)>=4)&&(col(recencyStr)<=6),4)
      .when((col(recencyStr)>=7)&&(col(recencyStr)<=9),3)
      .when((col(recencyStr)>=10)&&(col(recencyStr)<=15),2)
      .when(col(recencyStr)>=16,1)
      .as(recencyStr)

    //计算F的分数
    var getFrequencyScore: Column =functions.when(col(frequencyStr) >= 200, 5)
      .when((col(frequencyStr) >= 150) && (col(frequencyStr) <= 199), 4)
      .when((col(frequencyStr) >= 100) && (col(frequencyStr) <= 149), 3)
      .when((col(frequencyStr) >= 50) && (col(frequencyStr) <= 99), 2)
      .when((col(frequencyStr) >= 1) && (col(frequencyStr) <= 49), 1)
      .as(frequencyStr)


    //计算M的分数
    var getMonetaryScore: Column =functions.when(col(monetaryStr) >= 200000, 5)
      .when(col(monetaryStr).between(100000, 199999), 4)
      .when(col(monetaryStr).between(50000, 99999), 3)
      .when(col(monetaryStr).between(10000, 49999), 2)
      .when(col(monetaryStr) <= 9999, 1)
      .as(monetaryStr)

    // 2、计算RFM的分数
    val getRFMScoreDF: DataFrame = getRFMDF.select('memberId ,getRecencyScore,getFrequencyScore,getMonetaryScore)

    println("--------------------------------------------------")
    //getRENScoreDF.show()

/* +---------+-------+---------+--------+
| memberId|recency|frequency|monetary|
+---------+-------+---------+--------+
| 13822725|      2|        3|       4|
| 13823083|      2|        3|       5|
|138230919|      2|        3|       5|
| 13823681|      2|        3|       4|
*/
    // 3、将数据转换成向量

    val RFMFeature: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencyStr, frequencyStr, monetaryStr))
      .setOutputCol(featureStr)
      .transform(getRFMScoreDF)

    RFMFeature.show()
/* +---------+-------+---------+--------+-------------+
| memberId|recency|frequency|monetary|      feature|
+---------+-------+---------+--------+-------------+
| 13822725|      2|        3|       4|[2.0,3.0,4.0]|
| 13823083|      2|        3|       5|[2.0,3.0,5.0]|
|138230919|      2|        3|       5|[2.0,3.0,5.0]|
| 13823681|      2|        3|       4|[2.0,3.0,4.0]|
|  4033473|      2|        3|       5|[2.0,3.0,5.0]| */

    // 4、数据分类
    val model: KMeansModel = new KMeans()
      .setK(7) // 设置7类
      .setMaxIter(5) // 迭代计算5次
      .setFeaturesCol(featureStr) // 设置特征数据
      .setPredictionCol("featureOut") // 计算完毕后的标签结果
      .fit(RFMFeature)

    // 将其转换成 DF
    val modelDF: DataFrame = model.transform(RFMFeature)

    modelDF.show()
/*+---------+-------+---------+--------+-------------+----------+
| memberId|recency|frequency|monetary|      feature|featureOut|
+---------+-------+---------+--------+-------------+----------+
| 13822725|      2|        3|       4|[2.0,3.0,4.0]|         1|
| 13823083|      2|        3|       5|[2.0,3.0,5.0]|         0|
|138230919|      2|        3|       5|[2.0,3.0,5.0]|         0|
| 13823681|      2|        3|       4|[2.0,3.0,4.0]|         1|

截止到目前，用户的分类已经完毕，用户和对应的类别已经有了
缺少类别与标签ID的对应关系
这个分类完之后，featureOut的 0-6 只表示7个不同的类别，并不是标签中的 0-6 的级别
*/
    modelDF.groupBy("featureOut")
        .agg(max(col("recency")+col("frequency")+col("monetary")) as "max",
          min(col("recency")+col("frequency")+col("monetary")) as "min").show()

/*
+----------+---+---+
|featureOut|max|min|
+----------+---+---+
|         1|  9|  9|
|         6|  6|  6|
|         3|  9|  7|
|         5|  5|  4|
|         4| 12| 11|
|         2|  3|  3|
|         0| 10| 10|
+----------+---+---+
*/

    println("===========================================")

    //5、分类排序  遍历所有的分类(0-6)
    //获取每个类别内的价值（）中心点包含的所有点的总和就是这个类的价值
    //model.clusterCenters.indices   据类中心角标
    //model.clusterCenters(i)  具体的某一个类别（簇）

    val clusterCentersSum: immutable.IndexedSeq[(Int, Double)] = for(i <- model.clusterCenters.indices) yield (i,model.clusterCenters(i).toArray.sum)
    val clusterCentersSumSort: immutable.IndexedSeq[(Int, Double)] = clusterCentersSum.sortBy(_._2).reverse


    clusterCentersSumSort.foreach(println)
 /*
(4,11.038461538461538)
(0,10.0)
(1,9.0)
(3,8.0)
(6,6.0)
(5,4.4)
(2,3.0)
*/

    // 获取到每种分类及其对应的索引
    val clusterCenterIndex: immutable.IndexedSeq[(Int, Int)] = for(a <- clusterCentersSumSort.indices) yield (clusterCentersSumSort(a)._1,a)
    clusterCenterIndex.foreach(println)
    /*
    类别的价值从高到底
    角标是从0-6
    (4,0)
    (0,1)
    (1,2)
    (3,3)
    (6,4)
    (5,5)
    (2,6)
     */

    //6、分类数据和标签数据join
    // 将其转换成DF
    val clusterCenterIndexDF: DataFrame = clusterCenterIndex.toDF("type","index")

    // 开始join
    val JoinDF: DataFrame = fiveTagDF.join(clusterCenterIndexDF,fiveTagDF.col("rule") ===  clusterCenterIndexDF.col("index"))

    println("- - - - - - - -")
    JoinDF.show()
/*+---+----+----+-----+
| id|rule|type|index|
+---+----+----+-----+
|169|   0|   4|    0|
|170|   1|   0|    1|
|171|   2|   1|    2|
|172|   3|   3|    3|
|173|   4|   6|    4|
|174|   5|   5|    5|
|175|   6|   2|    6|
+---+----+----+-----+*/
    val fiveTageList: List[TagRule] = JoinDF.map(row => {

      val id: String = row.getAs("id").toString
      val types: String = row.getAs("type").toString

      TagRule(id.toInt, types)
    }).collectAsList() // 将DataSet转换成util.List[TagRule]   这个类型遍历时无法获取id,rule数据
      .asScala.toList

    println("- - - - - - - -")

    //7、获得数据标签（udf）
    // 需要自定义UDF函数
    val getRFMTags: UserDefinedFunction = udf((featureOut: String) => {
      // 设置标签的默认值
      var tagId: Int = 0
      // 遍历每一个五级标签的rule
      for (tagRule <- fiveTageList) {
        if (tagRule.rule == featureOut) {
          tagId = tagRule.id
        }
      }
      tagId
    })

    val CustomerValueTag: DataFrame = modelDF.select('memberId .as("userId"),getRFMTags('featureOut).as("tagsId"))

    println("*****************************************")

    CustomerValueTag.show(false)

    println("*****************************************")


    //8、表现写入hbase
    CustomerValueTag
  }


  def main(args: Array[String]): Unit = {

    exec()

  }
}
