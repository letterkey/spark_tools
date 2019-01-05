package study.plugin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by yinmuyang on 18-12-12 16:07.
  */
class A ( val sc:SparkContext) extends Serializable {

  def testA[T](first:String)( implicit conf : SparkConf = sc.getConf): RDD[String] ={
    println("plugin"+conf.getAppId)

    null
  }
}

object A  {
  implicit def addTest(sc :SparkContext) = new A(sc)

  def main(args: Array[String]): Unit = {

    // 创建包对象 plugin extends XY 即可引用饮食转换函数
    val m = math.max("3", 2)
    println(m)
  }

}
