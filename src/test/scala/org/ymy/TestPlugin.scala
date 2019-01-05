package org.ymy

import study.plugin.A._
import org.apache.spark.sql.SparkSession

/**
  * Created by yinmuyang on 18-12-12 16:11.
  */
object TestPlugin {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.testA("11")
  }

}
