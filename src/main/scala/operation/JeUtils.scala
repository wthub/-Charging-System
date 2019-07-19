package operation

import connectPool.JedisConnectionPool
import org.apache.spark.rdd.RDD

class JeUtils extends Serializable {

  def sum(rdd:RDD[String]):Unit= {
    val logs: RDD[(String, Double)] = rdd.map(x => {
      val str = x.split(" ")
      val goods = str(2)
      val gMoney = str(4).toDouble
      (goods, gMoney)
    })
    val goodM = logs.reduceByKey(_+_).collect()
    val jedis =JedisConnectionPool.getConnection()
    for (t<-goodM){
      jedis.incrByFloat(t._1,t._2)
    }
    jedis.close()
  }


}
