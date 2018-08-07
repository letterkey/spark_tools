package study.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 同一个文件夹下不同schema的parquet file以相同column进行 合并
  * Created by YMY on 18/4/27.
  */
object MergeSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local")
      .appName("MergeSchema")
      .getOrCreate()

    import spark.sqlContext.implicits._
    val df1 = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    val df2 = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")

    df1.printSchema()
    df2.printSchema()

    df1.write.parquet("data/test_table/key=1")
    df2.write.parquet("data/test_table/key=2")
    val df3 = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    df3.printSchema()
    df3.show(20,false)
  }

}
