package study.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 隐式转换的目标：
  * 1.对目标类型类型转换
  * 2. 对目标增强操作方法（扩展 func）
  * 隐式转换是spark中常用的添加rdd等的操作函数
  * Created by yinmuyang on 18-12-12 16:42.
  */
class MuYangFunctions (rdd:RDD[SalesRecord]) {

  // 自定义隐式转换函数
  def totalSales = rdd.map(_.itemValue).sum

  // 自定义RDD
  def discount(discountPercentage:Double) = new MuYangRDD(rdd,discountPercentage)
}

class MuYangFunctions_Start (sc : SparkContext) {

  // 自定义RDD 返回ＲＤＤ
  def read_text_file(file_path:String) = new MuYangRDD_start(sc,file_path)
}

object MuYangFunctions {
  implicit def addMuYangFunctions(rdd: RDD[SalesRecord]) = new MuYangFunctions(rdd)
  implicit def addMuYangFunctions(sc : SparkContext) = new MuYangFunctions_Start(sc)
}

case class SalesRecord(
                        val id: String,
                        val customerId: String,
                        val itemId: String,
                        val itemValue: Double
                      ) extends Serializable
