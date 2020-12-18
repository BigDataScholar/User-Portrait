package com.alice.test

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
 * @Author: Alice菌
 * @Date: 2020/7/2 15:03
 * @Description: 

    用决策树对鸢尾花数据进行分类

 */
object IrisTree {

  def main(args: Array[String]): Unit = {
    // 监督学习如何实现
    // 训练集，测试集如何使用
    // 成功率（准确率）

    // 1、初始化SparkSession
    val spark: SparkSession = SparkSession.builder().appName("IrisTree").master("local[*]").getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 导入隐式转换
    import spark.implicits._

    // 2、读取数据
    val dataSource: DataFrame = spark.read
      .csv("file:///E:\\2020大数据\\BigData\\企业级360°全方位用户画像【讲义】\\课堂资料\\0619\\03挖掘型标签\\数据集\\iris_tree.csv")
      .toDF("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width", "Species")
      .select('Sepal_Length cast DoubleType,   //字符串类型无法直接计算，需要将其传为doubleType
        'Sepal_Width cast DoubleType,
        'Petal_Length cast DoubleType,
        'Petal_Width cast DoubleType,
        'Species
      )

    //展示一些数据
    dataSource.show()
    //+------------+-----------+------------+-----------+-----------+
    //|Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|    Species|
    //+------------+-----------+------------+-----------+-----------+
    //|         5.1|        3.5|         1.4|        0.2|Iris-setosa|
    //|         4.9|        3.0|         1.4|        0.2|Iris-setosa|
    //|         4.7|        3.2|         1.3|        0.2|Iris-setosa|
    //|         4.6|        3.1|         1.5|        0.2|Iris-setosa|
    //|         5.0|        3.6|         1.4|        0.2|Iris-setosa|
    //|         5.4|        3.9|         1.7|        0.4|Iris-setosa|
    //|         4.6|        3.4|         1.4|        0.3|Iris-setosa|

    // 3、标签处理（数据不能直接使用，需要将最终的标签处理成数字。）
    val SpeciesToLabel: StringIndexerModel = new StringIndexer()
      .setInputCol("Species")
      .setOutputCol("label")
      .fit(dataSource)

    //4、将特征数据处理成向量
    val features: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width"))
      .setOutputCol("features")

    //5、初始化决策树，进行分类
    val decisionTree: DecisionTreeClassifier = new DecisionTreeClassifier() //创建决策树对象
      .setFeaturesCol("features") //设置特征列
      .setPredictionCol("prediction") //设置预测后的列名
      .setMaxDepth(4)  //设置深度

    //------查看----------

    //6、构建一个 PipeLine  将 第3  4  5 步骤连起来
    val pipeline: Pipeline = new Pipeline().setStages(Array(SpeciesToLabel,features,decisionTree))

    //7、将原始数据拆分成训练集和测试集
    //将数据切分成80%的训练集和20%的测试集
    val Array(trainDatas,testDatas): Array[Dataset[Row]] = dataSource.randomSplit(Array(0.8,0.2))


    //8、使用 PipLine 对训练集进行训练，使用测试集进行测试
    //使用训练数据进行训练，得到一个模型
    val model: PipelineModel = pipeline.fit(trainDatas)

    //使用测试集进行测试
    val testDF: DataFrame = model.transform(testDatas)
    testDF.show()
    //+------------+-----------+------------+-----------+---------------+-----+-----------------+--------------+-------------+----------+
    //|Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|        Species|label|         features| rawPrediction|  probability|prediction|
    //+------------+-----------+------------+-----------+---------------+-----+-----------------+--------------+-------------+----------+
    //|         4.3|        3.0|         1.1|        0.1|    Iris-setosa|  0.0|[4.3,3.0,1.1,0.1]|[38.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|
    //|         4.4|        3.2|         1.3|        0.2|    Iris-setosa|  0.0|[4.4,3.2,1.3,0.2]|[38.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|
    //|         4.7|        3.2|         1.3|        0.2|    Iris-setosa|  0.0|[4.7,3.2,1.3,0.2]|[38.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|
    //|         4.9|        3.0|         1.4|        0.2|    Iris-setosa|  0.0|[4.9,3.0,1.4,0.2]|[38.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|
    //|         5.0|        2.0|         3.5|        1.0|Iris-versicolor|  1.0|[5.0,2.0,3.5,1.0]|[0.0,35.0,0.0]|[0.0,1.0,0.0]|       1.0|
    //|         5.0|        3.2|         1.2|        0.2|    Iris-setosa|  0.0|[5.0,3.2,1.2,0.2]|[38.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|
    //|         5.1|        3.4|         1.5|        0.2|    Iris-setosa|  0.0|[5.1,3.4,1.5,0.2]|[38.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|

    //9、查看分类的成功的百分比
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator() //多类别评估器
      .setLabelCol("label") //设置原始数据中自带的label
      .setPredictionCol("prediction") //设置根据数据计算出来的结果“prediction”

    val Score: Double = evaluator.evaluate(testDF)   //将测试的数据集DF中自带的结果与计算出来的结果进行对比得到最终分数
    println(">>>>>>>>>>>>>>>>>>>>>>>  "+Score)
    //>>>>>>>>>>>>>>>>>>>>>>>  0.9669208211143695

    val decisionTreeClassificationModel: DecisionTreeClassificationModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    // 输出决策树
    println(decisionTreeClassificationModel.toDebugString)

    //DecisionTreeClassificationModel (uid=dtc_d936bd9c91b5) of depth 4 with 13 nodes
    //  If (feature 2 <= 1.9)
    //   Predict: 0.0
    //  Else (feature 2 > 1.9)
    //   If (feature 2 <= 4.7)
    //    If (feature 3 <= 1.6)
    //     Predict: 1.0
    //    Else (feature 3 > 1.6)
    //     Predict: 2.0
    //   Else (feature 2 > 4.7)
    //    If (feature 2 <= 4.9)
    //     If (feature 3 <= 1.5)
    //      Predict: 1.0
    //     Else (feature 3 > 1.5)
    //      Predict: 2.0
    //    Else (feature 2 > 4.9)
    //     If (feature 3 <= 1.6)
    //      Predict: 2.0
    //     Else (feature 3 > 1.6)
    //      Predict: 2.0


  }
}
