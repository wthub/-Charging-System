package operation
import scala.collection.mutable.ListBuffer
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object test {
   def main(args: Array[String]): Unit = {
     //    val ss = SparkSession.builder().master("local").appName("Test").getOrCreate()
     //    val rdd = ss.sparkContext.textFile("file:///D:\\demo\\1.txt").map(x=>(JSON.parseObject(x)))
     //    OPeration.recharge_Order_Quantity(rdd)
     //      val list = ListBuffer[Int]()
     //    list.append(1)
     //    list.append(5,2,3,4)
     //    for(i<-0 to list.size-1){
     //      if(list(i)==2){
     //        list.update(i,6)
     //      }
     //    }
     //    list.foreach(x=>println(x))
     val ss = SparkSession.builder().master("local").appName("test").getOrCreate()
     val df = ss.read.json("file:///D:\\A资料\\项目\\项目（二）01\\充值平台实时统计分析\\cmcc.json")
     df.createTempView("t")
     ss.sql("select count(1) from t where serviceName='reChargeNotifyReq' and provinceCode=311").show()
     ss.sql("select count(1) from t group by serviceName").show()

   }
}
