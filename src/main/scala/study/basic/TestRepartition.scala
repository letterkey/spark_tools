package study.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
 *rdd重分区N->M
 */
object TestRepartition {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
     sparkConf.setAppName("wordCount").setMaster("local")
     val sc = new SparkContext(sparkConf)
     //将1~100的数组分成两组
     val rdd = sc.makeRDD(1 to 100,100)
     println("rdd partitions:"+rdd.partitions.size)
     val repartitionRdd = rdd.repartition(4)
     println("repartitionRdd partitions:"+repartitionRdd.partitions.size)

     val coalesceRdd = rdd.coalesce(5)
     println("coalesceRdd partitions:"+coalesceRdd.partitions.size)

     val coalesceRdd_true = rdd.coalesce(6,true)
     println("coalesceRdd_true partitions:"+coalesceRdd_true.partitions.size)
     sc.stop()
   }
 }
