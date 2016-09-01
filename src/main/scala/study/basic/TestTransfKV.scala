package study.basic

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *KV型RDD转换
 */
object TestTransfKV {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
     sparkConf.setAppName("wordCount").setMaster("local")
     val sc = new SparkContext(sparkConf)
     //将1~100的数组分成两组
     val rdd = sc.parallelize(Array((1,1),(1,2),(2,1),(3,1)),1)
     val partitionByRDD = rdd.partitionBy(new HashPartitioner(2))

     val mapValuesRdd = rdd.mapValues(t => t + 1)
     mapValuesRdd.collect()

     val flatMapValuesRdd = rdd.flatMapValues(x => Seq(x,"v"))
     flatMapValuesRdd.collect()

     sc.stop()
   }
 }
