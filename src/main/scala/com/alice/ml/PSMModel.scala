package com.alice.ml

import com.alice.base.BaseModel
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.immutable

/*
 * @Author: Alice菌
 * @Date: 2020/6/26 11:17
 * @Description: 

    基于PSM模型,对用户的价格敏感度进行挖掘

 */

object PSMModel extends BaseModel {

  override def setAppName(): String = "PSMModel"

  override def setFourTagId(): String = "181"

  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    // 五级标签的数据
      fiveTagDF.show()
    //+---+----+
    //| id|rule|
    //+---+----+
    //|182|   1|
    //|183|   2|
    //|184|   3|
    //|185|   4|
    //|186|   5|
    //+---+----+


    // HBase的数据
       hbaseDF.show()
    //+---------+--------------------+-----------+---------------+
    //| memberId|             orderSn|orderAmount|couponCodeValue|
    //+---------+--------------------+-----------+---------------+
    //| 13823431|gome_792756751164275|    2479.45|           0.00|
    //|  4035167|jd_14090106121770839|    2449.00|           0.00|
    //|  4035291|jd_14090112394810659|    1099.42|           0.00|
    //|  4035041|amazon_7877495617...|    1999.00|           0.00|


    //只需要求取下面的字段，即可获取到最终的结果
    //优惠次数 
    //总购买次数
    //优惠总金额
    //应收总金额 = 优惠金额+成交金额

    // 引入隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._

    // 优惠次数
    val preferentialCount:String = "preferential"
    // 订单总数
    val orderCount:String = "orderCount"
    // 总优惠金额
    val couponCodeValue:String = "couponCodeValue"
    // 应收总金额
    val totalValue:String = "totalValue"

    //  优惠次数
    val getPreferentialCount:Column= count(
      when(col("couponCodeValue") !== 0.00,1)
    ) as preferentialCount

    // 总购买次数
    val getOrderCount: Column = count("orderSn") as orderCount

    // 优惠总金额
    val getCouponCodeValue:Column= sum("couponCodeValue") as couponCodeValue

    // 应收总金额
    val getTotalValue: Column = (sum("orderAmount") + sum("couponCodeValue") ) as totalValue

    // 特征单词
    val featureStr: String = "feature"  // 向量
    val predictStr: String = "predict"  // 分类

    // 进行查询
    val getPSMDF01: DataFrame = hbaseDF.groupBy("memberId")
      .agg(getPreferentialCount, getOrderCount, getCouponCodeValue,getTotalValue)

    //展示结果
    getPSMDF01.show(5)

    //+---------+------------+-----------+---------------+------------------+
    //| memberId|preferential|orderAmount|couponCodeValue|        totalValue|
    //+---------+------------+-----------+---------------+------------------+
    //|  4033473|           3|        142|          500.0|252430.91999999998|
    //| 13822725|           4|        116|          800.0|         180098.34|
    //| 13823681|           1|        108|          200.0|169946.09999999998|
    //|138230919|           3|        125|          600.0|240661.56999999998|
    //| 13823083|           3|        132|          600.0|         234124.17|
    //+---------+------------+-----------+---------------+------------------+

    // 先设置上我们常用的单词
    // 优惠订单占比
    val preferentiaOrderlPro:String = "preferentiaOrderlPro"
    // 平均优惠金额占比
    val avgPreferentialPro:String = "avgPreferentialPro"
    // 优惠金额占比
    val preferentialMoneyPro: String = "preferentialMoneyPro"

    /*  获取到想要的字段结果  */
       //  优惠订单占比(优惠订单数 / 订单总数)
    val getPSMDF02: DataFrame = getPSMDF01.select(col("memberId"),col("preferential") / col("orderCount") as preferentiaOrderlPro,
      //  平均优惠金额占比(平均优惠金额 / 平均每单应收金额)
      (col("couponCodeValue") / col("preferential")) / (col("totalValue") / col("orderCount")) as avgPreferentialPro,
      //  优惠金额占比(优惠总金额 / 订单总金额)
      col("couponCodeValue") / col("totalValue") as preferentialMoneyPro)

    getPSMDF02.show()
    //+---------+--------------------+-------------------+--------------------+
    //| memberId|preferentiaOrderlPro| avgPreferentialPro|preferentialMoneyPro|
    //+---------+--------------------+-------------------+--------------------+
    //|  4033473| 0.02112676056338028|0.09375502282631092|0.001980739918865724|
    //| 13822725|0.034482758620689655| 0.1288185110423561| 0.00444201762215021|
    //| 13823681|0.009259259259259259|0.12709912142732316|0.001176843716919...|
    //|138230919|               0.024|0.10388031624658645|0.002493127589918075|
    //| 13823083|0.022727272727272728|0.11276067737901643|0.002562742667704919|
    //| 13823431| 0.01639344262295082|0.13461458465166434|0.002206796469699...|
    //|  4034923|0.009259259259259259|0.12882071966768546|0.001192784441367458|
    //|  4033575|               0.032|0.07938713328518321|0.002540388265125...|
    //| 13822841|                 0.0|               null|                 0.0|


    val getPSMDF03: DataFrame = getPSMDF02.select(col("memberId"),col("preferentiaOrderlPro"),col("avgPreferentialPro"),col("preferentialMoneyPro"),(col("preferentiaOrderlPro")+col("avgPreferentialPro")+col("preferentialMoneyPro")) as "PSM" ).filter('PSM isNotNull)

    getPSMDF03.show()
    //+---------+--------------------+-------------------+--------------------+-------------------+
    //| memberId|preferentiaOrderlPro| avgPreferentialPro|preferentialMoneyPro|                PSM|
    //+---------+--------------------+-------------------+--------------------+-------------------+
    //|  4033473| 0.02112676056338028|0.09375502282631092|0.001980739918865724|0.11686252330855693|
    //| 13822725|0.034482758620689655| 0.1288185110423561| 0.00444201762215021|0.16774328728519597|
    //| 13823681|0.009259259259259259|0.12709912142732316|0.001176843716919...|0.13753522440350205|
    //|138230919|               0.024|0.10388031624658645|0.002493127589918075| 0.1303734438365045|
    //| 13823083|0.022727272727272728|0.11276067737901643|0.002562742667704919| 0.1380506927739941|


    // 为了方便K-Means计算，我们将数据转换成向量
    val PSMFeature: DataFrame = new VectorAssembler()
      .setInputCols(Array("PSM"))
      .setOutputCol(featureStr)
      .transform(getPSMDF03)


    PSMFeature.show()
    //+---------+--------------------+--------------------+--------------------+-------------------+--------------------+
    //| memberId|preferentiaOrderlPro|  avgPreferentialPro|preferentialMoneyPro|                PSM|             feature|
    //+---------+--------------------+--------------------+--------------------+-------------------+--------------------+
    //|  4033473| 0.02112676056338028| 0.09375502282631092|0.001980739918865724|0.11686252330855693|[0.11686252330855...|
    //| 13822725|0.034482758620689655|  0.1288185110423561| 0.00444201762215021|0.16774328728519597|[0.16774328728519...|
    //| 13823681|0.009259259259259259| 0.12709912142732316|0.001176843716919...|0.13753522440350205|[0.13753522440350...|
    //|138230919|               0.024| 0.10388031624658645|0.002493127589918075| 0.1303734438365045|[0.1303734438365045]|
    //| 13823083|0.022727272727272728| 0.11276067737901643|0.002562742667704919| 0.1380506927739941|[0.1380506927739941]|
    //| 13823431| 0.01639344262295082| 0.13461458465166434|0.002206796469699...|0.15321482374431458|[0.15321482374431...|
    //|  4034923|0.009259259259259259| 0.12882071966768546|0.001192784441367458|0.13927276336831218|[0.13927276336831...|
    //|  4033575|               0.032| 0.07938713328518321|0.002540388265125...|0.11392752155030907|[0.11392752155030...|
    //| 13823153|0.045112781954887216| 0.10559805877421218|0.004763822200340399|0.15547466292943982|[0.15547466292943...|


    // 利用KMeans算法，进行数据的分类
    val KMeansModel: KMeansModel = new KMeans()
      .setK(5) // 设置4类
      .setMaxIter(5) // 迭代计算5次
      .setFeaturesCol(featureStr) // 设置特征数据
      .setPredictionCol(predictStr) // 计算完毕后的标签结果
      .fit(PSMFeature)


    // 将其转换成DF
    val KMeansModelDF: DataFrame = KMeansModel.transform(PSMFeature)

    KMeansModelDF.show()
    //+---------+--------------------+--------------------+--------------------+-------------------+--------------------+-------+
    //| memberId|preferentiaOrderlPro|  avgPreferentialPro|preferentialMoneyPro|                PSM|             feature|predict|
    //+---------+--------------------+--------------------+--------------------+-------------------+--------------------+-------+
    //|  4033473| 0.02112676056338028| 0.09375502282631092|0.001980739918865724|0.11686252330855693|[0.11686252330855...|      4|
    //| 13822725|0.034482758620689655|  0.1288185110423561| 0.00444201762215021|0.16774328728519597|[0.16774328728519...|      4|
    //| 13823681|0.009259259259259259| 0.12709912142732316|0.001176843716919...|0.13753522440350205|[0.13753522440350...|      4|
    //|138230919|               0.024| 0.10388031624658645|0.002493127589918075| 0.1303734438365045|[0.1303734438365045]|      4|
    //| 13823083|0.022727272727272728| 0.11276067737901643|0.002562742667704919| 0.1380506927739941|[0.1380506927739941]|      4|
    //| 13823431| 0.01639344262295082| 0.13461458465166434|0.002206796469699...|0.15321482374431458|[0.15321482374431...|      4|
    //|  4034923|0.009259259259259259| 0.12882071966768546|0.001192784441367458|0.13927276336831218|[0.13927276336831...|      4|
    //|  4033575|               0.032| 0.07938713328518321|0.002540388265125...|0.11392752155030907|[0.11392752155030...|      4|

    // 计算用户的价值
    val clusterCentersSum: immutable.IndexedSeq[(Int, Double)] = for(i <- KMeansModel.clusterCenters.indices) yield (i,KMeansModel.clusterCenters(i).toArray.sum)
    val clusterCentersSumSort: immutable.IndexedSeq[(Int, Double)] = clusterCentersSum.sortBy(_._2).reverse

    clusterCentersSumSort.foreach(println)
    //(3,0.5563226557645843)
    //(1,0.31754213552513205)
    //(2,0.21020766974136296)
    //(4,0.131618637271183)
    //(0,0.08361272609460167)

    // 获取到每种分类以及其对应的索引
    val clusterCenterIndex: immutable.IndexedSeq[(Int, Int)] = for(a <- clusterCentersSumSort.indices) yield (clusterCentersSumSort(a)._1,a)
    clusterCenterIndex.foreach(println)
    //(3,0)
    //(1,1)
    //(2,2)
    //(4,3)
    //(0,4)

    // 类别的价值从高到低，角标依次展示
    // 将其转换成DF
    val clusterCenterIndexDF: DataFrame = clusterCenterIndex.toDF(predictStr,"index")
    clusterCenterIndexDF.show()
    //+-------+-----+
    //|predict|index|
    //+-------+-----+
    //|      3|    0|
    //|      1|    1|
    //|      2|    2|
    //|      4|    3|
    //|      0|    4|
    //+-------+-----+

    val JoinDF: DataFrame = fiveTagDF.join(clusterCenterIndexDF,fiveTagDF.col("rule") ===  clusterCenterIndexDF.col("index"))

    JoinDF.show()
    //+---+----+-------+-----+
    //| id|rule|predict|index|
    //+---+----+-------+-----+
    //|182|   0|      3|    0|
    //|183|   1|      1|    1|
    //|184|   2|      2|    2|
    //|185|   3|      4|    3|
    //|186|   4|      0|    4|
    //+---+----+-------+-----+

    // 获取到需要的字段
    val JoinDFS: DataFrame = JoinDF.select(predictStr,"id")

    // fiveTageList
    val fiveTageMap: Map[String, String] = JoinDFS.as[(String,String)].collect().toMap


    // 获得数据标签（udf）
    // 需要自定义UDF函数
    val getRFMTags: UserDefinedFunction = udf((featureOut: String) => {

      fiveTageMap.get(featureOut)
    })

    val PriceSensitiveTag: DataFrame = KMeansModelDF.select('memberId .as("userId"),getRFMTags('predict).as("tagsId"))

    PriceSensitiveTag.show()
    //+---------+------+
    //|   userId|tagsId|
    //+---------+------+
    //|  4033473|   185|
    //| 13822725|   185|
    //| 13823681|   185|
    //|138230919|   185|

    PriceSensitiveTag

  }

  def main(args: Array[String]): Unit = {

    exec()

  }
}
