package study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.test.LocalSQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 测试dataframe spark 1.3+
 * Created by Administrator on 2015/7/14.
 */
object DataFrame {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDDRelation")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("hdfs://xxx:9000/data/jsonfile")

    df.printSchema()
    //选择所有年龄大于21岁的人，只保留name字段
    df.filter(df("age") > 21).select("name").show()
    // 选择name，并把age年龄字段自增1
    df.select(df("name"),df("age") + 1).show()
    // 按年龄分组计数
    df.groupBy("age").count().show()

    // 强dataframe注册为临时表，使用sql进行查询
    df.registerTempTable("people")
    sqlContext.sql("select * from people").show()

  }
}
