package cn.tedu.dao

import cn.tedu.pojo.LogBean
import org.apache.hadoop.fs.shell.find.Result
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext

import scala.util.Random

object HBaseUtil {
  /*
  根据指定的时间范围和行键的正则表达式,查询HBase表数据
   */
  def queryByRange(sc: SparkContext, startTime: Long, endTime: Long, regex: String) = {

    val hbaseConf=HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum",
                  "hadoop01,hadoop02,hadoop03")

    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")

    hbaseConf.set(TableInputFormat.INPUT_TABLE,"weblog")

    //创建HBase扫描对象,设置扫描的范围
    val scan=new Scan()
    scan.setStartRow(startTime.toString.getBytes())
    scan.setStopRow(endTime.toString.getBytes())

    val filter=new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex))
    //绑定过滤器到scan对象
    scan.setFilter(filter)

    hbaseConf.set(TableInputFormat.SCAN,
                  Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
    //读取HBase表数据
    val resultRDD=sc.newAPIHadoopRDD(hbaseConf,
                                    classOf[TableInputFormat],
                                    classOf[ImmutableBytesWritable],
                                    classOf[org.apache.hadoop.hbase.client.Result])
    //返回查询的结果集RDD
    resultRDD

  }

  def save(sc: SparkContext, logBean: LogBean): Unit = {

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum",
                              "hadoop01,hadoop02,hadoop03")

    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")

    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"weblog")

    val job=new Job(sc.hadoopConfiguration)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //org.apache.hadoop.fs.shell.find.Result
    job.setOutputValueClass(classOf[Result])

    //指定输出的表类型
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //RDD[(key,value)]
    val hbaseRDD=sc.makeRDD(List(logBean)).map{bean=>
      //行键:sstime_uvid_ssid_cip_随机数字
      //①行键中以时间戳开头,在HBase中会按时间做升序排序,好处是可以按时间段范围做查询
      //②行键中包含了用户id,会话id,ip,便于后续利用行键过滤器来查询数据
      //③行键中包含随机数字,避免热点问题
      val rowKey=bean.sstime+"_"+bean.uvid+"_"+bean.ssid+"_"+bean.cip+"_"+Random.nextInt(100)

      //创建HBase行对象,并指定行键
      val put=new Put(rowKey.getBytes())

      put.add("cf1".getBytes(),"url".getBytes(),bean.url.getBytes())
      put.add("cf1".getBytes(),"urlname".getBytes(),bean.urlname.getBytes())
      put.add("cf1".getBytes(),"uvid".getBytes(),bean.uvid.getBytes())
      put.add("cf1".getBytes(),"ssid".getBytes(),bean.ssid.getBytes())
      put.add("cf1".getBytes(),"sscount".getBytes(),bean.sscount.getBytes())
      put.add("cf1".getBytes(),"sstime".getBytes(),bean.sstime.getBytes())
      put.add("cf1".getBytes(),"cip".getBytes(),bean.cip.getBytes())

      (new ImmutableBytesWritable(),put)
    }
    //执行写出
    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)


  }

}
