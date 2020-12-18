package com.alice.tools

import com.alice.bean.HBaseMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 真正读取HBase数据源的Relation
  */
class HBaseReadableRelation(context: SQLContext, meta: HBaseMeta) extends BaseRelation with TableScan with Serializable {
  //定义sqlContext
  override def sqlContext: SQLContext = context
  //定义数据结构的schema 里面定义列名/列的类型/当前列是否可以为空.
  override def schema: StructType = {
    //构建一个StructType,所有列的元数据信息(id:name,type,true   rule:name,type,false)
    //meta.selectFields => 从MySQL的4级标签获取到的.selectFields -> id,job
    val fields: Array[StructField] = meta.selectFields.split(",")
      .map(fieldName => {
        StructField(fieldName, StringType, nullable = true)
      })
    //使用Fields构建StructType
    StructType(fields)
  }

  /**
    * 构建数据源,我们可以自己定义从HBase中拿到的数据,封装为Row返回,
    * @return
    */
  override def buildScan(): RDD[Row] = {
    //数据在哪?HBase
    //我们要返回什么数据?RDD[Row]
    //定义HBase相关的conf
    val conf: Configuration = new Configuration()
    conf.set("hbase.zookeeper.property.clientPort", meta.zkPort)
    conf.set("hbase.zookeeper.quorum", meta.zkHosts)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, meta.hbaseTable)

    //从hadoop中构建我们的数据源RDD
    val sourceRDD: RDD[(ImmutableBytesWritable, Result)] = context.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    //获取Result数据
    val resultRDD: RDD[Result] = sourceRDD.map(_._2)
    //从result中获取我们需要的字段,别的字段都不要.
    //map == 将result => row
    resultRDD.map(result => {
      //获取列的名字,我们可以使用selectFields进行切割.
      val seq: Seq[String] = meta.selectFields.split(",")
        //将列名转换为row,一行数据
        //将String列名=> 具体的列的值.
        .map(fieldName => {
        //如果使用result获取数据,数据类型默认为byte数组,需要使用HBase的工具类Bytes将数据转换为String.
        Bytes.toString(result.getValue(meta.family.getBytes, fieldName.getBytes))
      }).toSeq
      //将列的值封装成row
      Row.fromSeq(seq)
    })
  }
}
