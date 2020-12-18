package com.alice.tools

import com.alice.bean.HBaseMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 数据写入的Relation
  */
class HBaseWritableRelation(context: SQLContext, meta: HBaseMeta, data: DataFrame) extends BaseRelation with InsertableRelation with Serializable {

  override def sqlContext: SQLContext = context

  override def schema: StructType = data.schema

  /**
    * 将data数据插入到HBase中.
    *
    * @param data
    * @param overwrite
    */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //构建HBase相关的配置.
    val config: Configuration = new Configuration()
    config.set("hbase.zookeeper.property.clientPort", meta.zkPort)
    config.set("hbase.zookeeper.quorum", meta.zkHosts)
    config.set("zookeeper.znode.parent", "/hbase-unsecure")
    config.set("mapreduce.output.fileoutputformat.outputdir", "/test01")
    config.set(TableOutputFormat.OUTPUT_TABLE, meta.hbaseTable)

    val job: Job = Job.getInstance(config)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    data
      //将DataFrame转换为RDD
      .rdd
      //将并行度设置为1
      .coalesce(1)
      //将每一行数据row => 插入HBase的put
      .map(row => {
      //向HBase中存入数据需要的是Put对象.
      //      new Delete(rowkey)
      //      new Get(rowkey)
      //构建put对象的时候需要指定rowkey,我们可以用用户ID作为rowkey.
      val rowkey: String = row.getAs("userId").toString
      val put: Put = new Put(rowkey.getBytes)
      //我们需要向put中插入列.
      meta.selectFields.split(",")
        .map(fieldName => {
          //获取当前列的值
          val value: String = row.getAs(fieldName).toString
          //向Put中添加列
          put.addColumn(meta.family.getBytes, fieldName.getBytes, value.getBytes)
        })
      (new ImmutableBytesWritable, put)
    })
      //将数据进行保存
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

