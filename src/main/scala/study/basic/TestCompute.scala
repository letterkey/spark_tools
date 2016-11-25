package study.basic

import org.apache.spark.{TaskContext, SparkConf, SparkContext}

/**
  * 测试RDD的分区计算
  */
object TestCompute {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
     sparkConf.setAppName("wordCount").setMaster("local")
     val sc = new SparkContext(sparkConf)
     //将1~100的数组分成两组
     val rdd = sc.parallelize(1 to 100,2)
     val filter3 = rdd.map(t => t+2).filter(t => t>3)

     val rddSize = rdd.partitions.size
     println("RDD partitions size is :"+rddSize)
     sc.stop()
   }
 }
