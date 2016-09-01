package study.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * spark中也有可能出现数据倾斜问题(如join等，当key有大部分相同时(如像hive数据倾斜那样join的字段很多为null))，
 * 所以需要查看各分区的元素数目来判断数据各分区分布情况
 * Created by root on 15-5-11.
 */
object RDDUtils {
  def getPartitionCounts[T](sc : SparkContext, rdd : RDD[T]) : Array[Long] = {
    sc.runJob(rdd, getIteratorSize _)
  }
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }
}
