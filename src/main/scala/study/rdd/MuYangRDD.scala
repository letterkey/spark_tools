package study.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

/**
  * Created by yinmuyang on 18-12-12 17:14.
  */
class MuYangRDD(prex:RDD[SalesRecord],after:Double) extends RDD[SalesRecord](prex){
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] = {
    firstParent[SalesRecord].iterator(split,context).map(sr =>{
      val discount = sr.itemValue*after
      new SalesRecord(sr.id,sr.customerId,sr.itemId,discount)
    })
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[SalesRecord].partitions
  }
}


/**
  * 自定义ＲＤＤ：上游对象ｐｒｅｘ为sparkcontext，即只有sparkcontext对象可以调用该函数
  * @param prex
  * @param file_path
  */
//class MuYangRDD_start(prex:SparkContext, file_path:String)
//  extends RDD[String](prex,Seq.empty)
//{
//  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] = {
//    Nil
//  }
//
//  override protected def getPartitions: Array[Partition] = {
//    firstParent[SalesRecord].partitions
//  }
//}