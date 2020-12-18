package com.alice.ml

import com.alice.base.BaseModel
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.immutable

/*
 * @Author: Alice菌
 * @Date: 2020/6/24 08:25
 * @Description: 

       用户的活跃度标签开发
 */
object RFEModel extends BaseModel{

  override def setAppName(): String = "RFEModel"

  override def setFourTagId(): String = "176"

  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    // 展示MySQL的五级标签数据
    fiveTagDF.show()
    //+---+----+
    //| id|rule|
    //+---+----+
    //|177|   1|
    //|178|   2|
    //|179|   3|
    //|180|   4|
    //+---+----+

    // 展示HBase中的数据
    hbaseDF.show(false)
    //+--------------+--------------------+-------------------+
    //|global_user_id|             loc_url|           log_time|
    //+--------------+--------------------+-------------------+
    //|           424|http://m.eshop.co...|2019-08-13 03:03:55|
    //|           619|http://m.eshop.co...|2019-07-29 15:07:41|
    //|           898|http://m.eshop.co...|2019-08-14 09:23:44|
    //|           642|http://www.eshop....|2019-08-11 03:20:17|


    //RFE三个单词
    //最近一次访问时间R
    val recencyStr: String = "recency"
    //访问频率 F
    val frequencyStr: String = "frequency"
    //页面互动度 E
    val engagementsStr: String = "engagements"

    // 特征单词
    val featureStr: String = "feature"  // 向量
    val predictStr: String = "predict"  // 分类
    // 计算业务数据
    // R(会员最后一次访问或到达网站的时间)
    // F(用户在特定时间周期内访问或到达的频率)
    // E(页面的互动度,注意:一个页面访问10次，算1次)

    // 引入隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    /* 分别计算 R F  E  的值 */

    // R 计算  最后一次浏览距今的时间
    val getRecency: Column = datediff(current_timestamp(),max("log_time")) as recencyStr

    // F 计算  页面访问次数(一个页面访问多次，就算多次)
    val getFrequency: Column = count("loc_url") as frequencyStr

    // E 计算 页面互动度(一个页面访问多次，只计算一次)
    val getEngagements: Column = countDistinct("loc_url") as engagementsStr

    val getRFEDF: DataFrame = hbaseDF.groupBy("global_user_id")
      .agg(getRecency, getFrequency, getEngagements)

    getRFEDF.show(false)
    //+--------------+-------+---------+-----------+
    //|global_user_id|recency|frequency|engagements|
    //+--------------+-------+---------+-----------+
    //|296           |312    |380      |227        |
    //|467           |312    |405      |267        |
    //|675           |312    |370      |240        |
    //|691           |312    |387      |244        |


    //现有的RFM 量纲不统一，需要执行归一化   为RFM打分
    //R 时间大于  08-17 = 2 分   小于 则 1分
    //F >= 400 则 4 分 ;300-399 则3分 ;200-299 则 2 分 ; 0-199 则 1分
    //E >= 40 则 4 分 ;30-29 则3分 ;20-29 则 2 分 ; 0-19 则 1分

    //计算R的分数
    val getRecencyScore: Column =
      when(col(recencyStr).between(0,15), 5)
        .when(col(recencyStr).between(16,30), 4)
        .when(col(recencyStr).between(31,45), 3)
        .when(col(recencyStr).between(46,60), 2)
        .when(col(recencyStr).gt(60), 1)
        .as(recencyStr)

    //计算F的分数
    val getFrequencyScore: Column =
      when(col(frequencyStr).geq(400), 5)
        .when(col(frequencyStr).between(300,399), 4)
        .when(col(frequencyStr).between(200,299), 3)
        .when(col(frequencyStr).between(100,199), 2)
        .when(col(frequencyStr).leq(99), 1)
        .as(frequencyStr)

    //计算E的分数
    val getEngagementScore: Column =
      when(col(engagementsStr).geq(250), 5)
        .when(col(engagementsStr).between(200,249), 4)
        .when(col(engagementsStr).between(150,199), 3)
        .when(col(engagementsStr).between(50,149), 2)
        .when(col(engagementsStr).leq(49), 1)
        .as(engagementsStr)

    // 计算 RFE 的分数
    val getRFEScoreDF: DataFrame = getRFEDF.select('global_user_id ,getRecencyScore,getFrequencyScore,getEngagementScore)

    getRFEScoreDF.show(false)
    //+--------------+-------+---------+-----------+
    //|global_user_id|recency|frequency|engagements|
    //+--------------+-------+---------+-----------+
    //|296           |1      |4        |4          |
    //|467           |1      |5        |5          |
    //|675           |1      |4        |4          |
    //|691           |1      |4        |4          |
    //|829           |1      |5        |5          |

    // 为了方便计算，我们将数据转换成向量
    val RFEFeature: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencyStr, frequencyStr, engagementsStr))
      .setOutputCol(featureStr)
      .transform(getRFEScoreDF)


    RFEFeature.show()
   //+--------------+-------+---------+-----------+-------------+
   //|global_user_id|recency|frequency|engagements|      feature|
   //+--------------+-------+---------+-----------+-------------+
   //|           296|      1|        4|          4|[1.0,4.0,4.0]|
   //|           467|      1|        5|          5|[1.0,5.0,5.0]|
   //|           675|      1|        4|          4|[1.0,4.0,4.0]|
   //|           691|      1|        4|          4|[1.0,4.0,4.0]|


    var  SSE: String =""
    //4  数据分类（不知道哪个类的活跃度高和低）
    for(k<-2  to 9){

      val model: KMeansModel = new KMeans()
        .setK(k)
        .setMaxIter(5)
        .setSeed(10)
        .setFeaturesCol(featureStr)
        .setPredictionCol(predictStr)
        .fit(RFEFeature)

      SSE=k+"-"+model.computeCost(RFEFeature)
      println(SSE)
    }


    // 利用KMeans算法，进行数据的分类
    val KMeansModel: KMeansModel = new KMeans()
      .setK(4) // 设置4类
      .setMaxIter(5) // 迭代计算5次
      .setFeaturesCol(featureStr) // 设置特征数据
      .setPredictionCol(predictStr) // 计算完毕后的标签结果
      .fit(RFEFeature)


    // 将其转换成DF
    val KMeansModelDF: DataFrame = KMeansModel.transform(RFEFeature)

    KMeansModelDF.show()
    //+--------------+-------+---------+-----------+-------------+-------+
    //|global_user_id|recency|frequency|engagements|      feature|predict|
    //+--------------+-------+---------+-----------+-------------+-------+
    //|           296|      1|        4|          4|[1.0,4.0,4.0]|      1|
    //|           467|      1|        5|          5|[1.0,5.0,5.0]|      0|
    //|           675|      1|        4|          4|[1.0,4.0,4.0]|      1|
    //|           691|      1|        4|          4|[1.0,4.0,4.0]|      1|


    // 计算用户的价值
    val clusterCentersSum: immutable.IndexedSeq[(Int, Double)] = for(i <- KMeansModel.clusterCenters.indices) yield (i,KMeansModel.clusterCenters(i).toArray.sum)
    val clusterCentersSumSort: immutable.IndexedSeq[(Int, Double)] = clusterCentersSum.sortBy(_._2).reverse

    clusterCentersSumSort.foreach(println)
    //(0,11.0)
    //(3,10.0)
    //(2,10.0)
    //(1,9.0)

    // 获取到每种分类以及其对应的索引
    val clusterCenterIndex: immutable.IndexedSeq[(Int, Int)] = for(a <- clusterCentersSumSort.indices) yield (clusterCentersSumSort(a)._1,a)
    clusterCenterIndex.foreach(println)
    //(0,0)
    //(3,1)
    //(2,2)
    //(1,3)


    // 类别的价值从高到低，角标依次展示
    // 将其转换成DF
    val clusterCenterIndexDF: DataFrame = clusterCenterIndex.toDF(predictStr,"index")
   clusterCenterIndexDF.show()
    //+-------+-----+
    //|predict|index|
    //+-------+-----+
    //|      0|    0|
    //|      3|    1|
    //|      2|    2|
    //|      1|    3|
    //+-------+-----+

    // 开始join
    val JoinDF: DataFrame = fiveTagDF.join(clusterCenterIndexDF,fiveTagDF.col("rule") ===  clusterCenterIndexDF.col("index"))

    JoinDF.show()
    //+---+----+-------+-----+
    //| id|rule|predict|index|
    //+---+----+-------+-----+
    //|177|   0|      0|    0|
    //|178|   1|      3|    1|
    //|179|   2|      2|    2|
    //|180|   3|      1|    3|
    //+---+----+-------+-----+

    val JoinDFS: DataFrame = JoinDF.select(predictStr,"id")

    //fiveTageList
   val fiveTageMap: Map[String, String] = JoinDFS.as[(String,String)].collect().toMap


    //7、获得数据标签（udf）
    // 需要自定义UDF函数
    val getRFMTags: UserDefinedFunction = udf((featureOut: String) => {

        fiveTageMap.get(featureOut)

    })

    val CustomerValueTag: DataFrame = KMeansModelDF.select('global_user_id .as("userId"),getRFMTags('predict).as("tagsId"))


    /* 另一种做法 */
    val newTags: DataFrame = JoinDF.join(KMeansModelDF, KMeansModelDF.col("predict") === JoinDF.col("predict"))
      .select('global_user_id.as("userId"), 'id.as("tagsId"))


    newTags.show()

    //CustomerValueTag.show(false)
    //|userId|tagsId|
    //+------+------+
    //|296   |180   |
    //|467   |177   |
    //|675   |180   |
    //|691   |180   |
    //|829   |177   |
    //|125   |180   |
    //|451   |180   |
    //|800   |180   |
    //|853   |179   |
    CustomerValueTag

  }


  def main(args: Array[String]): Unit = {

    exec()

  }
}
