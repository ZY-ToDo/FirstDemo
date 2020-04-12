package cn.tedu.service

import java.util.Calendar

import cn.tedu.dao.{HBaseUtil, MysqlUtil}
import cn.tedu.pojo.{LogBean, Tongji}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将SparkStreaming和Kafka整合，从kafka中消费数据
 */
object Driver {

  def main(args: Array[String]): Unit = {
    // 如果从Kafka消费数据,Local模式的线程数至少是2个
    //其中一个线程负责SparkStreaming ,另一个负责消费Kafka
    val conf=new SparkConf().setMaster("local[5]").setAppName("kafka")

    val sc=new SparkContext(conf)

    val ssc=new StreamingContext(sc,Seconds(5))

    //zookeeper集群地址
    val zkHosts="hadoop01:2181,hadoop02:2181,hadoop03:2181"
    //定义消费者组名
    val groupId="gp1"
    //指定消费的主题信息,key是主题名,value是消费的线程数
    //可以消费多个主题,比如:Map("weblog"->1,"enbook"->1)
    val topics=Map("weblog"->1)

    //通过整合包提供的工具类,从kafka指定主题中消费数据
    val kafkaSource=KafkaUtils.createStream(ssc,zkHosts,groupId,topics)
                              .map{x=>x._2}

    //实时代码处理：首先清洗出业务字段
    //foreachRDD方法是DStream的一种输出方法,将每一批的数据转变为RDD来进行处理
    kafkaSource.foreachRDD{rdd=>
      //将每一批的RDD数据,转为迭代器,存储了当前批次的所有行数据
      val lines=rdd.toLocalIterator

      while(lines.hasNext){
        //每迭代一次,获取一条数据
        val line=lines.next()
        //清洗字段:url urlname uvid ssid sscount sstime cip
        val info=line.split("\\|")
        val url=info(0)
        val urlname=info(1)
        val uvid=info(13)
        val ssid=info(14).split("_")(0)
        val sscount=info(14).split("_")(1)
        val sstime=info(14).split("_")(2)
        val cip=info(15)

        //用bean来封装业务字段,并写出到HBase表  这些业务字段只是需要用到，最后是业务指标字段，需要用这些字段计算得到
        val logBean=LogBean(url,urlname,uvid,ssid,sscount,sstime,cip)

        //实时统计各个业务指标:pv uv vv newIp newCust
        //接受到一条,就算作一个pv

        //下面是离线处理的注释
        //pv（页面访问量）用户每次访问，就算一个pv
        //UV（独立访客数）提示：我们是根据uvid来标识一个用户的
        //vv - 独立会话数 - 一天之内所有的会话的数量 - 一天之内ssid去重后的总数
        //newip - 新增ip总数 - 一天内所有ip去重后在历史数据中从未出现过的数量
        //newcust - 新增客户数 - 一天内所有的uvid去重后在历史数据中从未出现过的总数
        val pv=1

        //=============================这个1和0的作用，没有用户记录查出来返回1，说明是新来的，有记录则为0，
        //=============================返回1到mysql中，就可以执行增加操作，新来一个记录就更新增加1
        //=============================所以统计计数是在mysql中
        //难点:统计uv,uv的结果1 or 0.
        //处理思路:先获取当前访问记录的uvid,然后去HBase weblog表查询当天的数据
        //如果此uvid已经出现过,则uv=0; 反之,uv=1 只要有用户访问网页，就存进今天的数据，在今天再次访问，查询hbase里有记录，返回0
        //如何查询HBase表当天的数据,我们可以指定查询的范围:
        //StartTime=当天0:00的时间戳  EndTime=sstime
        val endTime=sstime.toLong

        val calendar=Calendar.getInstance()
        //以endTime时间戳为基准,找当天的0:00的时间戳
        calendar.setTimeInMillis(endTime)
        calendar.set(Calendar.HOUR,0)
        calendar.set(Calendar.MINUTE,0)
        calendar.set(Calendar.SECOND,0)
        calendar.set(Calendar.MILLISECOND,0)

        //通过微秒值来比较
        val startTime=calendar.getTimeInMillis

        //去HBase表,使用行键正则过滤器,根据uvid来匹配查询 匹配包含用户id的行键
        val uvRegex="^\\d+_"+uvid+".*$"

        val uvResultRDD=HBaseUtil.queryByRange(sc,startTime,endTime,uvRegex)

        //代表用户记录 如果没有查到用户数据，返回1
        val uv=if(uvResultRDD.count()==0) 1 else 0

        //统计vv  vv的结果1 or 0. 根据ssid去HBase表查询当天的数据
        //处理思路同uv
        val vvRegex="^\\d+_\\d+_"+ssid+".*$"
        val vvResultRDD=HBaseUtil.queryByRange(sc,startTime,endTime,vvRegex)

        val vv=if(vvResultRDD.count()==0) 1 else 0

        //统计newIp 新增ip的结果 1 or 0 .根据当前记录中的cip 去HBase表查询历史
        //如果没有查到,则newIp=1
        val newIpRegex="^\\d+_\\d+_\\d+_"+cip+".*$"
        val newIpResultRDD=HBaseUtil.queryByRange(sc,0,endTime,newIpRegex)

        val newIp=if(newIpResultRDD.count()==0) 1 else 0

        //统计newCust 根据当前记录中的uvid,去HBase表查询历史
        //如果没有查到,则newCust=1
        val newCustRDD=HBaseUtil.queryByRange(sc,0,endTime,uvRegex)
        val newCust=if(newCustRDD.count()==0) 1 else 0

        //通过bean来封装统计好的业务指标
        val tongjiBean=Tongji(sstime.toLong,pv,uv,vv,newIp,newCust)

        //将业务指标数据插入到mysql数据库
        MysqlUtil.save(tongjiBean)
        HBaseUtil.save(sc,logBean)


      }

    }

    ssc.start()

    ssc.awaitTermination()
  }
}
