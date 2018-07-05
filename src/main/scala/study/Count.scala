package study

import org.apache.spark.{SparkContext, SparkConf}

/**
 * hello word
 * Created by Administrator on 2014/9/23.
 */
object Count{
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("count").setMaster("local")
    val sc = new SparkContext(conf)
    val result = sc.parallelize(List(1,2,3,4,5,6,7):::List(8,9)).map(_*2)

    val count = result.count()
    val reduce = result.reduce(_+_)
    println(count + "_"+ reduce)
  }
}
