package operation

import com.alibaba.fastjson.{JSON, JSONObject}
import connectPool.JedisConnectionPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.dateUtils
import scala.collection.mutable.ListBuffer
/*
* 充值运营业务
 */


object OPeration {


  private val list = ListBuffer[((String,String),(Int,Double))]()

  //city文件广播初始化
  def apply:Broadcast[Map[String, String]]={
    //创建连接
    val ss = SparkSession.builder().appName("Apply").master("local").getOrCreate()
    //读取广播文件
    val line: RDD[String] = ss.sparkContext.textFile("file:///D:\\A资料\\项目\\项目（二）01\\充值平台实时统计分析\\city.txt")
    val map = line.map(x=>(x.split("\\s")(0),x.split("\\s")(1))).collect().toMap
    //广播变量
    val bro: Broadcast[Map[String, String]] = ss.sparkContext.broadcast(map)
    return bro
    }
  
  




  //统计全网的充值订单量，充值金额，充值成功数业务
  def recharge_Order_Quantity(rdd:RDD[JSONObject]):Unit={
    val orderRDD = rdd
      .filter(x => x.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
      .map(x => {
          //解析json串
          val orderid = x.get("orderId").toString //订单编号
          val serviceName = x.get("serviceName").toString //订单类型
          val bussinessRst = x.get("bussinessRst").toString //订单状态
          val chargefee = x.get("chargefee").toString.toDouble //充值金额
          val interFacRst = x.get("interFacRst").toString //接口状态
          val requestId = x.get("requestId").toString //订单流水时间
          val receiveNotifyTime = x.get("receiveNotifyTime").toString //订单结束时间
          (orderid, serviceName, bussinessRst, chargefee, interFacRst, requestId, receiveNotifyTime)
        })

        //过滤出充值订单、订单成功，充值金额数据
        val ach_order: RDD[(String, List[Double])] = orderRDD.map(x => {
          val time: Long = dateUtils.time_Difference(dateUtils.dateTotime(x._7), dateUtils.dateTotime(x._6)) //单个订单处理时间
          val succeed_order: Int = if (("0000").equalsIgnoreCase(x._3)) 1 else 0 //判断订单是否成功
          val money: Double = if (("0000").equalsIgnoreCase(x._3)) x._4 else 0.0 //订单金额
          (x._6.substring(0, 8), List[Double](1, succeed_order, money, time))
        }).reduceByKey((list1, list2) => {
          list1.zip(list2).map(t => t._1 + t._2)
        })
        //充值订单量，充值金额，充值成功数指标
        ach_order.foreachPartition(x => {
          val jedis = JedisConnectionPool.getConnection()
          x.foreach(t => {
            jedis.hincrBy(Constants.Constan.UP_ORDERS, t._1, t._2(0).toLong) //充值订单总数
            jedis.hincrBy(Constants.Constan.UP_SUCCEED_ORDER, t._1, t._2(1).toLong) //充值订单成功总数
            jedis.hincrByFloat(Constants.Constan.UP_MONEY_SUM, t._1, t._2(2)) //充值总金额
            jedis.hincrBy(Constants.Constan.UP_TIMES, t._1, t._2(3).toLong) //充值总时间
          })
          jedis.close()
        })
}




  //统计全网每分钟的订单量数据
  def minutes_orders(rdd: RDD[JSONObject]):Unit={
    //过滤出充值订单按分钟聚合
    val mintOrder: Array[(String, Int)] = rdd.filter(x =>
      ("reChargeNotifyReq").equalsIgnoreCase(x.get("serviceName").toString)).map(x => {
      (x.get("requestId").toString.substring(0, 12), 1)
    }).reduceByKey(_ + _).collect()
    //遍历存入redis中
    val jedis = JedisConnectionPool.getConnection()
    for(x<-mintOrder){
      jedis.hincrBy(Constants.Constan.UP_MINT_ORDERS,x._1,x._2)
    }
    jedis.close()
  }




  //统计每小时各个省份的充值失败数据量
  def failure_hours(rdd:RDD[JSONObject]):Unit={
    //过滤出失败的充值订单按小时聚合
    val failure_order_H: Array[(String, Int)] = rdd.filter(x =>
      ("reChargeNotifyReq").equalsIgnoreCase(x.get("serviceName").toString) && !(("0000").equalsIgnoreCase(x.get("bussinessRst").toString))).map(x => {
      (x.get("requestId").toString.substring(0, 10), 1)
    }).reduceByKey(_ + _).collect()
    //存入redis中
    val jedis = JedisConnectionPool.getConnection()
    for(x<-failure_order_H){
      jedis.hincrBy(Constants.Constan.UP_F_H_ORDERS,x._1,x._2)
    }
    jedis.close()
  }



  //以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数
  def province_top(rdd:RDD[JSONObject]):Unit= {
    val citys = apply.value
    val city_order: RDD[((String, String), List[Int])] = rdd.filter(x => (("reChargeNotifyReq").equalsIgnoreCase(x.get("serviceName").toString))).map(x => {
      val city = citys.getOrElse(x.get("provinceCode").toString, "未知") //订单所属省份
      val date = x.get("requestId").toString.substring(0, 8) //订单时间
      val succeed_order = if (("0000").equalsIgnoreCase(x.get("bussinessRst").toString)) 1 else 0 //判断订单是否成功
      ((city, date), List[Int](1, succeed_order))

    }).reduceByKey((list1, list2) => {//聚合流数据的总订单数和成功数
      list1.zip(list2).map(x => x._1 + x._2)
    })
    city_order.foreachPartition(x=>{
      val jdbc = utils.JDUtils.getConnections()
      val db = jdbc.createStatement()
      x.foreach(t=>{//遍历存入集合
        val sum: Int = t._2(0)
        val n:Int = t._2(1)
        val sql = "insert into province_order(province,`num`,n,times) values('"+t._1._1+"',"+sum+","+n+",'"+t._1._2+"')"
        db.execute(sql)
        if(list.size<1){//判断list中无数据直接存入
          list.append((t._1,(sum,n)))
        }else{
          var k=0
          for(i <- 0 to (list.size-1)){//遍历list有k相同的就累计赋值
            if(t._1._1.equalsIgnoreCase(list(i)._1._1) && t._1._2.equalsIgnoreCase(list(i)._1._2)) {

              list.update(i,(t._1,(sum+list(i)._2._1,(n+list(i)._2._2))))
              k+=1
            }
          }
          if(k==0){//无相同k直接存入
            list.append((t._1,(sum,n)))
          }
        }
      })
      utils.JDUtils.resultConn(jdbc)
    })
    //每次省份订单数累计的结果集降序排序
    val l = list.sortBy(-_._2._1)
    //获取jdbc连接对象
    val jdbc = utils.JDUtils.getConnections()
    //获取执行对象
    val st = jdbc.createStatement()
    //当list中不超过10条数据就全部存入
    if(l.size<10){
      //先清空上次排名表进行更新
      val clean = "truncate table charging.province_order_top10"
      st.execute(clean)
      for(f<-l){
        //实时省份订单成功率
        val d: Double = f._2._2.toDouble/f._2._1.toDouble
        //逐条插入
        val sql = "insert into province_order_top10(province,sum,succeed_percentage,times) values('"+f._1._1+"',"+f._2._1+","+d+",'"+f._1._2+"')"
        st.execute(sql)
      }
    }else{
      //清除上次排名更新实时数据
      val clean = "truncate table charging.province_order_top10"
      st.execute(clean)
      //超过10条数据取Top10遍历存储
      for(f<-l.take(10)){
        //实时省份订单成功率
        val d: Double = f._2._2.toDouble/f._2._1.toDouble
        val sql = "insert into province_order_top10(province,sum,succeed_percentage,times) values('"+f._1._1+"',"+f._2._1+","+d+",'"+f._1._2+"')"
        st.execute(sql)
      }
    }
    utils.JDUtils.resultConn(jdbc)
  }




  //实时统计每小时的充值笔数和充值金额
  def sum_monys_hours(rdd:RDD[JSONObject]):Unit={
    //过滤出充值成功订单
    val h_RDD: RDD[(String, List[Double])] = rdd.filter(x => (("reChargeNotifyReq").equalsIgnoreCase(x.get("serviceName").toString)) && (("0000").equalsIgnoreCase(x.get("bussinessRst").toString))).map(x => {
      (x.get("requestId").toString.substring(0, 10), List[Double](1, x.get("chargefee").toString.toDouble)) //取出时间（小时），订单数，充值金额
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })//按时间聚合
    //把流数据结果集遍历存入mysql数据库
    h_RDD.foreachPartition(x=>{
      val con = utils.JDUtils.getConnections()
      x.foreach(t=>{
        val stm = con.createStatement()
        val sql ="insert into hours_order_moneys(times,order_sum,order_moneys) values('"+t._1+"',"+t._2(0)+","+t._2(1)+")"
        stm.execute(sql)

      })
       utils.JDUtils.resultConn(con)
    })
  }


}

