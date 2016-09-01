package study

import org.apache.spark.SparkContext._

import org.apache.spark.{SparkContext,SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 *
 */
object TopK_2 {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
     sparkConf.setAppName("HdfsTest").setMaster("local")

     val sc = new SparkContext(sparkConf);
     val file = sc.textFile("hdfs://master:9000/usr/input/w*.txt");

     val reduce = file.flatMap(line => line.split(" ")).map(word=> (word,1)).reduceByKey(_ + _)
     val topk = reduce.mapPartitions(ite =>{
       // 遍历一个分区内的每个元素
       while (ite.hasNext){
          putToHeap(ite.next())
       }

       getHeap().iterator
     }).collect()
     for (x <- topk)
       println(x._1+"__________"+x._2)
     sc.stop()
   }
  var tmp = new ArrayBuffer[(String, Int)](10)
  def putToHeap(ite : (String,Int)){
    // 遍历一个分区内的数值
    println(ite._1+"+"+ite._2)
    tmp.+=(ite)
    tmp.sortWith(_._2 < _._2)
    tmp = tmp.takeRight(10)
//    if(tmp.length == 10)
//      for(x <- tmp)
//        println(x._1+"_______"+x._2)
  }

  def getHeap() : ArrayBuffer[(String,Int)] = {
    tmp
  }
 }
