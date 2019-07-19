package utils


import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar

object dateUtils {

  def dateTotime(str:String):Long={
    val s = str.substring(0,17)
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val date = format.parse(s)
    val time = date.getTime
    return time
  }
  def time_Difference(t1:Long,t2:Long):Long={
    return (t1-t2)/1000
  }
}
