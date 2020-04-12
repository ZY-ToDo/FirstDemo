package cn.tedu.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从HBase表读取数据
 */
object ReadDriver {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[3]").setAppName("readHBase")
    val sc=new SparkContext(conf)
    //创建HBase的环境参数对象
    val hbaseConf=HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    //指定读取的HBase表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"tb1")
    //创建HBase的扫描对象
    val scan=new Scan()
    //扫描的起始行键
    scan.setStartRow("2".getBytes())
    //扫描的终止行键
    scan.setStopRow("4".getBytes())
    //设置scan对象
    hbaseConf.set(TableInputFormat.SCAN,
                  Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    //读取HBase表,并将整表数据封装返回到RDD中
    //RDD[(key,value)] HBase表中的每一行数据,存在了value里
    //这个Result导入的是org.apache.hadoop.hbase.client包
    val resultRDD=sc.newAPIHadoopRDD(hbaseConf,
                                    classOf[TableInputFormat],
                                    classOf[ImmutableBytesWritable],
                                    classOf[Result])
    resultRDD.foreach{case(key,value)=>
        val name=value.getValue("cf1".getBytes(),"name".getBytes())
        val age=value.getValue("cf1".getBytes(),"age".getBytes())
        println(new String(name)+":"+new String(age))
    }

  }
}
