package study.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
 *测试RDD的location
 */
object TestLocations {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
     sparkConf.setAppName("wordCount").setMaster("local")
     val sc = new SparkContext(sparkConf)
     //将1~100的数组分成两组
     val file = sc.textFile("hdfs://10.128.17.21:9000/test/CHANGES.txt")
     val hadoopRdd = file.dependencies(0).rdd
     val hadoopRddPartitionCount = hadoopRdd.partitions.size
     val array = hadoopRdd.preferredLocations(hadoopRdd.partitions(0))
     println(array)
     sc.stop()
   }
 }
