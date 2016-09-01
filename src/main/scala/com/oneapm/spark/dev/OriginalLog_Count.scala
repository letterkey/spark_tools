
package com.oneapm.spark.dev

/*
  统计originformat 日志中的status值为true和false的记录总数
 */


import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object OriginalLog_Count {
  def main(args: Array[String]) {
//    if (args.length < 3) {
//      System.err.println("Usage: input <hdfs path>|output<hdfs path>|reduceNum<reduce total>")
//      System.exit(1)
//    }

    val sparkConf = new SparkConf()
    sparkConf.setAppName("HdfsTest").setMaster("local")
    val sc = new SparkContext(sparkConf);
    val file = sc.textFile("hdfs://localhost:9000/test/originformmat.txt");
    val data = file.map(line => {
      var jsonData = ""
      val lineData = line.split("\\|")
      val dataList = JSON.parseFull(lineData(1)).get.asInstanceOf[List[List[Any]]]
      if(dataList.size >= 2){
        var data2 = dataList(1)
        if(data2.size >=5 ){
          jsonData = data2.take(5).mkString(",")
        }
      }
      var map = JSON.parseFull(lineData(0)).get.asInstanceOf[Map[String,String]]
      map += ("jsonData" -> jsonData)
      map
    })

    var trueData = data.filter(m =>m.get("status").get == true)
    // 统计正常的个数
    var trueCount = trueData.count()
    // 统计各个版本的agent个数
    val trueVCount = trueData.map(m => {
      val tmpMap = m.asInstanceOf[Map[String,String]]
      val jsonData = tmpMap.get("jsonData").get.split(",")
      val key = jsonData.head+"_"+jsonData.last
      (key,1)
    }).reduceByKey(_+_,1)

    // ------------------------统计错误信息------------------------------------
    var falseData = data.filter(m => m.get("status").get == false)
    // 错误总数
    val falseCount = falseData.count()
    // 错误原因统计
    val falseVCount = falseData.map(m => (m.get("reason").get,1)).reduceByKey(_+_,1)
    // -------------------------统计每个token使用agent的版本变化---------------
    val tokenData = data.map(m => {
      val t = m.get("token").get+"_"+m.get("jsonData").get.split(",").last
      t
    }).distinct().collect().sortBy(d => d).foreach(println(_))
//
//    println("正确总记录数:"+trueCount)
//    trueVCount.foreach(println(_))
//
//    println("错误总记录数:"+falseCount)
//    falseVCount.foreach(println(_))


//
//    val reduce = file.flatMap(line => line.split(" ")).map(word=> (word,1)).reduceByKey(_ + _,1)
//
//     reduce.saveAsTextFile("hdfs://localhost:9000/opt/result")
    sc.stop()
  }
}
