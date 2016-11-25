package study.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 测试dataframe spark 1.3+
 * Created by Administrator on 2015/7/14.
 */
object DataFrameReadJSON {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDDRelation")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // 加载两个不同schema的json文件
    sqlContext.setConf("parquet.enable.summary-metadata", "false")
    val df = sqlContext.read.json("file:///Users/yinmuyang/git/fittime/spark_tools/data/json/merge-json1.txt")

    df.printSchema()

    df.write.mode(SaveMode.Append).option("mergeSchema", "true").save("file:///Users/yinmuyang/git/fittime/spark_tools/data/json/tmp/xx.parquet")

    val df2 = sqlContext.read.json("file:///Users/yinmuyang/git/fittime/spark_tools/data/json/merge-json2.txt")
    df2.printSchema()
    df2.write.mode(SaveMode.Append).option("mergeSchema", "true").save("file:///Users/yinmuyang/git/fittime/spark_tools/data/json/tmp/xx.parquet")

    // 读取parquet文件
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("file:///Users/yinmuyang/git/fittime/spark_tools/data/json/tmp/xx.parquet")
    df3.printSchema()
    df3.show()
    // 写入到一个partition文件中  (小文件合并)
    df3.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).save("file:///Users/yinmuyang/git/fittime/spark_tools/data/json/tmp/yy.parquet")
    // 加载测试文件
    val df4 = sqlContext.read.parquet("file:///Users/yinmuyang/git/fittime/spark_tools/data/json/tmp/yy.parquet")
    df4.show(false)
    println(sc.applicationId)
  }
}
