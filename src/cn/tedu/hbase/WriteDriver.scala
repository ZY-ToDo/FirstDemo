package cn.tedu.hbase

import org.apache.hadoop.fs.shell.find.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark和HBase整合,将数据写出到指定的HBase表中
 */
object WriteDriver {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[3]").setAppName("writeHBase")
    val sc=new SparkContext(conf)
    //指定zookeeper集群的ip地址
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum",
                               "hadoop01,hadoop02,hadoop03")
    //指定zookeeper通信的端口号
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    //指定向HBase写出的表名
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"tb1")
    //创建mapreduce
    val job=new Job(sc.hadoopConfiguration)
    //指定写出数据时,输出key类型   不可变的字节可序列化
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //指定写出数据时,输出value类型 org.apache.hadoop.fs.shell.find.Result
    job.setOutputValueClass(classOf[Result])
    //指定输出的表类型
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val data=sc.makeRDD(List("1 tom 23","2 rose 18","3 jim 25","4 jary 30"))

    //为了向HBase插入数据,需要 RDD[String]->RDD[(输出key,输出value)]
    val hbaseRDD=data.map{line=>
      val info=line.split(" ")
      val id=info(0)
      val name=info(1)
      val age=info(2)
      //创建一个hbase的行对象,并指定行键
      val put=new Put(id.getBytes())
      //向每行数据中,指定的列族和指定列插入数据
      put.add("cf1".getBytes(),"name".getBytes(),name.getBytes())
      put.add("cf1".getBytes(),"age".getBytes(),age.getBytes())
      (new ImmutableBytesWritable(),put)
    }
    //执行插入数据 调用这个才将数据写入
    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
