package study

import org.apache.spark.SparkContext._

import org.apache.spark.{SparkContext,SparkConf}

/**
 * 单节点topk
 */
object TopK {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
     sparkConf.setAppName("HdfsTest").setMaster("local")

     val sc = new SparkContext(sparkConf);
     val file = sc.textFile("hdfs://localhost:9000/usr/input/w*.txt");

     val reduce = file.flatMap(line => line.split(" ")).map(word=> (word,1)).reduceByKey(_ + _,1).map(w =>(w._2,w._1)).
       sortByKey(false,1).map(w => (w._2,w._1)).top(10).map(w => println(w._1+" "+w._2))
     sc.stop()
   }
 }
