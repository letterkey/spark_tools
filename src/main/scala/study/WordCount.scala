package study

import org.apache.spark.SparkContext._

import org.apache.spark.{SparkContext,SparkConf}

object WordCount {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
       sparkConf.setAppName("wordCount").setMaster("local")
 //    sparkConf.setMaster("spark://192.168.56.102:7077")
 //    sparkConf.setSparkHome(System.getenv("SPARK_HOME"))
     val sc = new SparkContext(sparkConf)
     val file = sc.textFile("hdfs://10.128.17.21:9000/test/CHANGES.txt");

     val reduce = file.flatMap(line => line.split(" ")).map(word=> (word,1)).reduceByKey(_ + _,1)
//     println(reduce.count())
//     reduce.mapPartitions()
     reduce.saveAsObjectFile("hdfs://10.128.17.21:9000/out/xxx")
//      reduce.saveAsTextFile("result")

     sc.stop()
   }
 }
