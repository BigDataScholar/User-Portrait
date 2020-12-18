package com.alice.tools

import com.alice.bean.HBaseMeta
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * 自定义HBase数据源
  */
class HBaseDataSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister with Serializable {


  /**
    * 读取数据源.
    *
    * @param sqlContext
    * @param parameters 在调用当前DataSource的时候传入的option键值对.
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    //将parameters里面的HBase相关的数据都取出来.
    //将map封装为HBaseMeta对象
    val meta: HBaseMeta = parseMeta(parameters)
    new HBaseReadableRelation(sqlContext, meta)
  }

  /**
    * 将数据写入到指定的位置,数据落地
    * @param sqlContext
    * @param mode 覆盖/追加
    * @param parameters 构建数据源的时候添加的参数.
    * @param data 需要保存的数据
    * @return
    */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    //先解析参数,将参数构建成HBaseMeta
    val meta: HBaseMeta = parseMeta(parameters)
    //创建HBaseWritableRelation对象
    val relation: HBaseWritableRelation = new HBaseWritableRelation(sqlContext, meta, data)
    relation.insert(data, overwrite = true)
    //将对返回
    relation
  }

  /**
    * 将Map转换为HBaseMeta
    * @param params
    * @return
    */
  def parseMeta(params: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      params.getOrElse(HBaseMeta.INTYPE, ""),
      params.getOrElse(HBaseMeta.ZKHOSTS, ""),
      params.getOrElse(HBaseMeta.ZKPORT, ""),
      params.getOrElse(HBaseMeta.HBASETABLE, ""),
      params.getOrElse(HBaseMeta.FAMILY, ""),
      params.getOrElse(HBaseMeta.SELECTFIELDS, ""),
      params.getOrElse(HBaseMeta.ROWKEY, "")
    )
  }
  override def shortName(): String = "hbase"
}
