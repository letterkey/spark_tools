package study.infobright

import java.io.ByteArrayInputStream

import org.apache.spark.{SparkConf, SparkContext}

object HdfsToInfoBright {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("HdfsToInfoBright").setMaster("local")
    //    sparkConf.setMaster("spark://192.168.56.102:7077")
    //    sparkConf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(sparkConf);
    val file = sc.textFile("hdfs://localhost:9000/tmp/scala.txt");

    val collect = file.collect()
    var sb = new StringBuffer()

    val list = collect.toList.map(li => sb.append(li).append("\n"))
    println(sb.toString)
    val is = new ByteArrayInputStream(sb.toString.getBytes())
    study.infobright.Mysql.excuteLoadData(is)
    //    println(reduce.count())
//    reduce.saveAsTextFile("/opt/result")

    sc.stop()
  }
}
