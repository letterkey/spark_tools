package org.ymy

import org.apache.spark.SparkContext
import study.rdd.MuYangFunctions._
import study.rdd.SalesRecord

/**
  * Created by yinmuyang on 18-12-12 16:46.
  */
object TestFunctions {
  def main(args: Array[String]): Unit = {
    val file_path = "file:///www/iteblog.csv"
    val sc = new SparkContext(args(0), "iteblogRDDExtending")
    val dataRDD = sc.textFile(file_path)
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),
        colValues(2),colValues(3).toDouble)
    })

    // 隐式转换函数:action
    salesRecordRDD.totalSales

    // 自定义rdd返回RDD ：transform
    salesRecordRDD.discount(11.0)

    // 调用自定义RDD
    sc.read_text_file(file_path)

  }

}
