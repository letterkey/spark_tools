package com.oneapm.spark.utils.json

/**
 * Created by root on 15-1-29.
 */
import scala.util.parsing.json._
object Test {
  def main (args: Array[String]) {

    val data = """[[237,945261],["Android","4.4.4","samsung SM-N9100","Android Agent","1.0.5","171cc02d-2d5f-4cd8-ad32-7fe72a5d20f3","CN","310000","samsung",{"size":"normal"},"中国","上海市"],0.0,[],[[{"scope":"","name":"Memory/Used"},{"count":1,"total":10.818359375,"min":10.818359375,"max":10.818359375,"sum_of_squares":117.03689956665039}],[{"scope":"","name":"Supportability/AgentHealth/Collector/Harvest"},{"count":1,"total":0.31700000166893005,"min":0.31700000166893005,"max":0.31700000166893005,"sum_of_squares":0.10048900105810166}]],[],[],[]]"""
    val data1 ="""{"dataType":"Data","status":true,"timestamp":1422514549730,"token":"AF399285AA6856521FA496EB3D47134063"}"""
    val l1 = JSON.parseFull(data).get.asInstanceOf[List[List[Any]]]
    println(l1(1)(1))


    val l2 = JSON.parseFull(data1).get.asInstanceOf[Map[String,String]]
    println(l2.get("status"))

    val data2 ="""{"dataType":"Data","status":true,"timestamp":1422514508404,"token":"AF399285AA6856521FA496EB3D47134063"}|[[237,264640],["Android","4.1.1","Meizu M031","Android Agent","1.0.5","746e395e-9f73-4b4d-ae89-384f3b5c53fd","CN","440000","Meizu",{"size":"normal"},"中国","广东省"],0.0,[],[[{"scope":"","name":"Memory/Used"},{"count":1,"total":16.125,"min":16.125,"max":16.125,"sum_of_squares":260.015625}],[{"scope":"","name":"Supportability/AgentHealth/Collector/Harvest"},{"count":1,"total":0.16300000250339508,"min":0.16300000250339508,"max":0.16300000250339508,"sum_of_squares":0.026569000816106803}]],[],[],[]]"""
    println(data2.split("\\|")(0))
    val lm = JSON.parseFull(data2.split("\\|")(0)).get.asInstanceOf[Map[String,String]]
    println(lm.get("dataType").get=="Data")
    println(lm.get("status").get==true)
//    println(l==x)

    val t = List(1,2,3)
    println(t.mkString(","))
  }
}
