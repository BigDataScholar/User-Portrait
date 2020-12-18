package com.alice.bean

case class HBaseMeta  (
                      inType:String,
                      zkHosts:String,
                      zkPort:String ,
                      hbaseTable:String ,
                      family:String,
                      selectFields:String,
                      rowKey: String
                    )


object HBaseMeta{
  val INTYPE: String = "inType"
  val ZKHOSTS: String = "zkHosts"
  val ZKPORT: String = "zkPort"
  val HBASETABLE: String = "hbaseTable"
  val FAMILY: String = "family"
  val SELECTFIELDS: String = "selectFields"
  val ROWKEY: String = "rowKey"

}
