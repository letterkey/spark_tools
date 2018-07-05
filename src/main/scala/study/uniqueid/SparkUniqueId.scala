package study.uniqueid

import org.apache.spark.sql.SparkSession

/**
  * 全局生成唯一ID
  * Created by YMY on 18/2/28.
  */
object SparkUniqueId {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("test_UniqueId").master("local").getOrCreate()
    val data = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7):::List(8,9))
          .map(m => m+"_"+m)
          .repartition(4)
//      .zipWithIndex()
      .zipWithUniqueId()
    data.collect().foreach(println)
  }
}
