package study

import java.util.Properties

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
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
    val d = b_i.map(r =>(r.getString(0),String.valueOf(r.getInt(1))))
        .reduceByKey(new org.apache.spark.HashPartitioner(2),(x,y) =>x+","+y)
        .filter(_._2.split(",").length > 1).persist(StorageLevel.MEMORY_ONLY)
    d.collect().foreach(println(_))
    val fpg = new FPGrowth()
        .setMinSupport(0.01)
        .run(d.map(_._2.split(",").distinct))
    // 求频繁项集
    fpg.freqItemsets.collect().foreach{
      itemset => println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
    }
    //发掘关联规则
    fpg.generateAssociationRules(0.00001)
        .collect().foreach(rule =>{
      println(rule.antecedent.mkString("前件[", ",", "]") // 前件
          + " => " + rule.consequent.mkString("后件[", ",", "]") // 后件
          + ", " + rule.confidence) // 可信度
    })
  }
}
