package study

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * hello word
 * Created by Administrator on 2014/9/23.
 */
object SparkMysql{
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("count").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val url = "jdbc:mysql://192.168.100.66:3306/eshop?useUnicode=true&characterEncoding=utf8&autoReconnect=true"
    val table = "TB_ORDER"
    val predicates:Array[String] = Array(" PAY_METHOD IS NOT NULL")
    val prop = new Properties()
    prop.setProperty("user","fittime")
    prop.setProperty("password","Fittime1991")
    val order = ssc.read.jdbc(url,table,predicates,prop)
    order.show(2)
    val p2:Array[String] = Array(" 1=1 ")
    val order_item = ssc.read.jdbc(url,"TB_ORDER_ITEM",p2,prop)
    order_item.show(2)
    val b_i = order_item.join(order,order("ORDER_ID")===order_item("ORDER_ID"),"inner").select("BUY_ACCOUNT_ID","GOODS_ID")
    b_i.printSchema()
    b_i.groupBy("BUY_ACCOUNT_ID").count().where("count > 1").show(100)
  }
}
