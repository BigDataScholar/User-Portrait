package com.alice.ml

import com.alice.base.BaseModel
import com.alice.bean.HBaseMeta
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType

/*
 * @Author: Alice菌
 * @Date: 2020/7/3 09:10
 * @Description:
 *
 *  基于  USG 模型开发 用户的购物性别 标签
    
 */
object USGModel extends BaseModel{

  override def setAppName(): String = "USGModel"

  override def setFourTagId(): String = "187"

  override def getNewTag(spark: SparkSession, fiveTagDF: DataFrame, hbaseDF: DataFrame): DataFrame = {

    fiveTagDF.show(5)
    //+---+----+
    //| id|rule|
    //+---+----+
    //|188|   0|
    //|189|   1|
    //|190|   2|
    //+---+----+

    hbaseDF.show(5)
    //+--------+--------------------+
    //|memberId|             orderSn|
    //+--------+--------------------+
    //|13823431|gome_792756751164275|
    //| 4035167|jd_14090106121770839|
    //| 4035291|jd_14090112394810659|
    //| 4035041|amazon_7877495617...|
    //|13823285|jd_14092120154435903|
    //+--------+--------------------+


    // 获取订单表数据
    val tbl_orders: DataFrame = hbaseDF

    // 本需求需要两张表，目前只有一个表 tbl_orders， tbl_goods 需要读取
    // 读取HBase中另一个商品表的数据
    val tbl_goods: DataFrame = spark.read.format("com.alice.tools.HBaseDataSource")
      .option("zkHosts","192.168.10.20")
      .option(HBaseMeta.ZKPORT, "2181")
      .option(HBaseMeta.HBASETABLE, "tbl_goods")
      .option(HBaseMeta.FAMILY,"detail")
      .option(HBaseMeta.SELECTFIELDS,"cOrderSn,ogColor,productType")
      .load()

    tbl_goods.show(5,truncate = false)
    //+----------------------+---------+-----------+
    //|cOrderSn              |ogColor  |productType|
    //+----------------------+---------+-----------+
    //|jd_14091818005983607  |白色       |烤箱         |
    //|jd_14091317283357943  |香槟金      |冰吧         |
    //|jd_14092012560709235  |香槟金色     |净水机        |
    //|rrs_15234137          |梦境极光【布朗灰】|烤箱         |
    //|suning_790750687478116|梦境极光【卡其金】|4K电视       |
    //+----------------------+---------+-----------+

    // 将HBase的订单表和商品表根据 订单id 【orderSn】 进行一个关联
    val orders_goods: DataFrame = tbl_orders.join(tbl_goods, tbl_orders.col("orderSn") === tbl_goods.col("cOrderSn"))
      .select("memberId", "productType", "ogColor")

    orders_goods.show(5)
    //+--------+-----------+-------+
    //|memberId|productType|ogColor|
    //+--------+-----------+-------+
    //|13823535|其他         |灰色     |
    //|13823535|智能电视       |银色     |
    //|13823535|燃气热水器      |粉色     |
    //|13823391|冰吧         |乐享金    |
    //|4034493 |LED电视      |金色     |
    //+--------+-----------+-------+

    // 引入隐式转换
    import spark.implicits._
    //引入sparkSQL的内置函数
    import org.apache.spark.sql.functions._


    // 现在
    // 数据/特征已经有了，但是缺少标签（啥样是女？啥样是男？）
    // 向业务部门咨询， 啥样是男，啥样是女
    val label: Column = functions
      .when('ogColor.equalTo("樱花粉")
        .or('ogColor.equalTo("白色"))
        .or('ogColor.equalTo("香槟色"))
        .or('ogColor.equalTo("香槟金"))
        .or('productType.equalTo("料理机"))
        .or('productType.equalTo("挂烫机"))
        .or('productType.equalTo("吸尘器/除螨仪")), 1) //女
      .otherwise(0)//男
      .alias("gender")

    //决策树算法需要的特征数据不能是字符串类型，但是我们的数据是字符串
    //所以我们需要将这里的字符串特征变为数值类型
    //颜色ID应该来源于字典表,这里简化处理

    //这里的编号，最好是数据库中读取
    val color: Column = functions
      .when('ogColor.equalTo("银色"), 1)
      .when('ogColor.equalTo("香槟金色"), 2)
      .when('ogColor.equalTo("黑色"), 3)
      .when('ogColor.equalTo("白色"), 4)
      .when('ogColor.equalTo("梦境极光【卡其金】"), 5)
      .when('ogColor.equalTo("梦境极光【布朗灰】"), 6)
      .when('ogColor.equalTo("粉色"), 7)
      .when('ogColor.equalTo("金属灰"), 8)
      .when('ogColor.equalTo("金色"), 9)
      .when('ogColor.equalTo("乐享金"), 10)
      .when('ogColor.equalTo("布鲁钢"), 11)
      .when('ogColor.equalTo("月光银"), 12)
      .when('ogColor.equalTo("时尚光谱【浅金棕】"), 13)
      .when('ogColor.equalTo("香槟色"), 14)
      .when('ogColor.equalTo("香槟金"), 15)
      .when('ogColor.equalTo("灰色"), 16)
      .when('ogColor.equalTo("樱花粉"), 17)
      .when('ogColor.equalTo("蓝色"), 18)
      .when('ogColor.equalTo("金属银"), 19)
      .when('ogColor.equalTo("玫瑰金"), 20)
      .otherwise(0)
      .alias("color")


    //类型ID应该来源于字典表,这里简化处理
    val productType: Column = functions
      .when('productType.equalTo("4K电视"), 9)
      .when('productType.equalTo("Haier/海尔冰箱"), 10)
      .when('productType.equalTo("Haier/海尔冰箱"), 11)
      .when('productType.equalTo("LED电视"), 12)
      .when('productType.equalTo("Leader/统帅冰箱"), 13)
      .when('productType.equalTo("冰吧"), 14)
      .when('productType.equalTo("冷柜"), 15)
      .when('productType.equalTo("净水机"), 16)
      .when('productType.equalTo("前置过滤器"), 17)
      .when('productType.equalTo("取暖电器"), 18)
      .when('productType.equalTo("吸尘器/除螨仪"), 19)
      .when('productType.equalTo("嵌入式厨电"), 20)
      .when('productType.equalTo("微波炉"), 21)
      .when('productType.equalTo("挂烫机"), 22)
      .when('productType.equalTo("料理机"), 23)
      .when('productType.equalTo("智能电视"), 24)
      .when('productType.equalTo("波轮洗衣机"), 25)
      .when('productType.equalTo("滤芯"), 26)
      .when('productType.equalTo("烟灶套系"), 27)
      .when('productType.equalTo("烤箱"), 28)
      .when('productType.equalTo("燃气灶"), 29)
      .when('productType.equalTo("燃气热水器"), 30)
      .when('productType.equalTo("电水壶/热水瓶"), 31)
      .when('productType.equalTo("电热水器"), 32)
      .when('productType.equalTo("电磁炉"), 33)
      .when('productType.equalTo("电风扇"), 34)
      .when('productType.equalTo("电饭煲"), 35)
      .when('productType.equalTo("破壁机"), 36)
      .when('productType.equalTo("空气净化器"), 37)
      .otherwise(0)
      .alias("productType")

    // 将用户数据中的字符串转为数值并添加标签
    val orders_goods_Int: DataFrame = orders_goods.select('memberId,productType,color,label)

    // 展示结果
    orders_goods_Int.show(5,truncate = false)
    //+--------+-----------+-----+------+
    //|memberId|productType|color|gender|
    //+--------+-----------+-----+------+
    //|13823535|0          |16   |0     |
    //|13823535|24         |1    |0     |
    //|13823535|30         |7    |0     |
    //|13823391|14         |10   |0     |
    //|4034493 |12         |9    |0     |
    //+--------+-----------+-----+------+


    // 3、将数据中自带的标签处理成数值
    val labelInt: StringIndexerModel = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("label")
      .fit(orders_goods_Int)

    // 4、特征向量化
    val features: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("productType", "color"))
      .setOutputCol("features")

    // 5、实例化决策树
    val decisionTree: DecisionTreeClassifier = new DecisionTreeClassifier() //  创建决策树对象
      .setFeaturesCol("features")    //  向量
      .setPredictionCol("prediction")  // 输出的结果
      .setMaxDepth(5)


    // 6、创建PipLine，添加 3 4 5 步 到pipLine
    val pipeline: Pipeline = new Pipeline().setStages(Array(labelInt,features,decisionTree))

    // 7、将数据分类，分成训练集和测试集
    //将数据切分成80%的训练集和20%的测试集
    val Array(trainDatas,testDatas): Array[Dataset[Row]] = orders_goods_Int.randomSplit(Array(0.8,0.2))

    //8、使用 PipLine 对训练集进行训练，使用测试集进行测试
    //使用训练数据进行训练，得到一个模型
    val model: PipelineModel = pipeline.fit(trainDatas)
    //测试模型
    val testDF: DataFrame = model.transform(testDatas)
    testDF.show()
    //+---------+-----------+-----+------+-----+-----------+--------------+--------------------+----------+
    //| memberId|productType|color|gender|label|   features| rawPrediction|         probability|prediction|
    //+---------+-----------+-----+------+-----+-----------+--------------+--------------------+----------+
    //| 13822725|         28|   20|     0|  0.0|[28.0,20.0]|  [5564.0,0.0]|           [1.0,0.0]|       0.0|
    //| 13822727|         25|    8|     0|  0.0| [25.0,8.0]| [16759.0,0.0]|           [1.0,0.0]|       0.0|
    //| 13822747|         17|    1|     0|  0.0| [17.0,1.0]|  [7700.0,0.0]|           [1.0,0.0]|       0.0|
    //| 13822781|         20|   18|     0|  0.0|[20.0,18.0]| [780.0,399.0]|[0.66157760814249...|       0.0|
    //| 13822789|          0|   10|     0|  0.0| [0.0,10.0]| [22381.0,0.0]|           [1.0,0.0]|       0.0|
    //| 13822789|          0|   18|     0|  0.0| [0.0,18.0]|  [7499.0,0.0]|           [1.0,0.0]|       0.0|
    //| 13822789|         10|   17|     1|  1.0|[10.0,17.0]|  [0.0,4975.0]|           [0.0,1.0]|       1.0|
    //| 13822819|         22|    8|     1|  1.0| [22.0,8.0]|  [0.0,2328.0]|           [0.0,1.0]|       1.0|

    // 9、查看模型的精确度
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator() //多类别评估器
      .setLabelCol("label") //设置原始数据中自带的label
      .setPredictionCol("prediction") //设置根据数据计算出来的结果“prediction”

    // 获取模型的精准度
    val Score: Double = evaluator.evaluate(testDF)   //将测试的数据集DF中自带的结果与计算出来的结果进行对比得到最终分数
    println(">>>>>>>>>>>>>>>>>>>>>>>  "+Score)
    //      >>>>>>>>>>>>>>>>>>>>>>>  0.9713653872994347


    // 查看决策树
    val decisionTreeClassificationModel: DecisionTreeClassificationModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    // 输出决策树
    println(decisionTreeClassificationModel.toDebugString)

    // 对用户ID进行分组，计算商品男性的百分比，和女性的百分比
    // 计算的时候需要使用所有的数据
    val trainDF: DataFrame = model.transform(trainDatas)
    // 两个数据拼接
    val manWomanAll: DataFrame = trainDF.union(testDF)
      .select('memberId,
        when('prediction === 0, 1).otherwise(0) as "man", //当prediction为0时，为man返回1，其他字段返回0
        when('prediction === 1, 1).otherwise(0) as "woman" //当prediction为1时，为woman返回1，其他字段返回0
      )
      //+--------+---+-----+
      //|memberId|man|woman|
      //+--------+---+-----+
      //|13822713|  1|    0|
      //|13822713|  0|    1|
      //|13822723|  1|    0|
      //|13822725|  1|    0|
      //|13822727|  1|    0|
      .groupBy("memberId")
      .agg(
        sum('man) cast DoubleType as "manSum",
        sum('woman) cast DoubleType  as "womanSum",
        count('memberId) cast DoubleType  as "all"
      )

    manWomanAll.show()
    //+---------+------+--------+---+
    //| memberId|manSum|womanSum|all|
    //+---------+------+--------+---+
    //| 13822725|  82.0|    32.0|114|
    //|  4033473| 126.0|    32.0|158|
    //| 13823083| 108.0|    38.0|146|
    //|138230919|  88.0|    29.0|117|
    //| 13823681| 105.0|    32.0|137|

    // 将五级标签转换成Map
    // 将五级标签数据转换为Map
    val fiveTagMap: Map[String, Long] = fiveTagDF.collect().map(row=>{
      (row(1).toString,row(0).toString.toLong)
    }).toMap


    fiveTagMap.foreach(println)
    //(0,188)
    //(1,189)
    //(2,190)


    //编写UDF函数，通过传入购买的男性和女性的商品次数,来判断用户的购买性别
    val getSexTag: UserDefinedFunction =udf((manSum :Double, womanSum :Double, all:Double)=>{

      //计算男性商品百分比
      val manPercent: Double =manSum / all
      //计算女性商品百分比
      val womPercent: Double =womanSum / all


      if(manPercent>=0.6){
        fiveTagMap("0")   // 男
      }else if(womPercent>=0.6){
        fiveTagMap("1")   // 女
      }else{
        fiveTagMap("2")   // 未知
      }
    })

    val newTags: DataFrame = manWomanAll.select('memberId as "userId",getSexTag('manSum,'womanSum,'all) as "tagsId")

    // 展示新数据的结果
    newTags.show()

    println("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")

    newTags

  }

  def main(args: Array[String]): Unit = {

    // 调用方法
    exec()

  }
}
