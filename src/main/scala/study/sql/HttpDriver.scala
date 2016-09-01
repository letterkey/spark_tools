package study.sql

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import study.utils._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//数据建模
//case class Http(ts:Long,        // 0
//                totalNum:Int,
//                totalTime:Double,
//                byteSend:String,
//                byteReceived:String,
//                appId:Int,          //5
//                appVersionId:String,
//                osId:String,
//                osVersionId:String,
//                deviceTypeId:String,
//                manufacturerId:String,//10
//                modelId:String,
//                countryCodeId:String,
//                regionCodeId:String,
//                carriesId:String,
//                domainId:String,      //15
//                domain:String,
//                urlId:String,
//                url:String,
//                statusCode:String,
//                errorCode:String,
//                deviceId:String)

object HttpDriver {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("http_sql").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext._

//    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    val http = sc.textFile("hdfs://localhost:9000/tmp/log.1420*75192_limit").map(line =>{
                  val tmp = line.split("\t")
                  val arrayBuffer = tmp.toBuffer
                  // 格式化时间戳
//  arrayBuffer(0) = format(arrayBuffer(0).toLong).toString
                  arrayBuffer
              }).map(o =>{
                  val sep="_"
                  // appId,appVersionId,osId,osVersionId,deviceTypeId,manufacturerId,modelId,countryCodeId,regionCodeId,carriesId,domainId,urlId,statusCode,errorCode
                  var key = o(5)+sep+o(6)+sep+o(7)+sep+o(8)+sep+o(9)+sep+o(10)+sep+o(11)+sep+o(12)+sep+o(13)+sep+o(14)+sep+o(15)+sep+o(17)+sep+o(19)+sep+o(20)+sep+o(21);
                  (key,o)
              })

    val kv = http.reduceByKey((a,b)=>{
      var array = b
      array(1)=(a(1).toInt+b(1).toInt).toString
      array(2)=(a(2).toDouble+b(2).toDouble).toString
      array(3)=(a(3).toDouble+b(3).toDouble).toString
      array(4)=(a(4).toDouble+b(4).toDouble).toString
      array
    })

    val d = kv.collectAsMap()
    val s1 = d.values.toList.sortBy(_(0))
    toMysql(s1)

  }

  /**
   * 根据给定的时间戳，获取指定时间格式的时间戳
   * @param time：1420611923369l
   * @param f  ："yyyy-MM-dd HH"
   * @return
   */
  def format(time:Long,f:String = "yyyy-MM-dd HH"): Long ={
    val format = new SimpleDateFormat(f)
    val s = format.format(new Date(time))
    val m = format.parse(s).getTime
    return m
  }

  def toMysql(data:List[mutable.Buffer[String]])={
    if(data.length > 0){
      var sql : String = "INSERT INTO httptransaction(timestamp,total_num,total_time,byte_send," +
        "byte_received,app_id,app_version_id,os_id,os_version_id,device_type_id," +
        "manufacturer_id,model_id,country_code_id,region_code_id,carries_id,domain_id,url_id,status_code,error_code)" +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
//      MySql.batchInsert(sql,data)
    }
  }

  //case class Http(ts:Long,        // 0
  //                totalNum:Int,
  //                totalTime:Double,
  //                byteSend:String,
  //                byteReceived:String,
  //                appId:Int,          //5
  //                appVersionId:String,
  //                osId:String,
  //                osVersionId:String,
  //                deviceTypeId:String,
  //                manufacturerId:String,//10
  //                modelId:String,
  //                countryCodeId:String,
  //                regionCodeId:String,
  //                carriesId:String,
  //                domainId:String,      //15
  //                domain:String,
  //                urlId:String,
  //                url:String,
  //                statusCode:String,
  //                errorCode:String,
  //                deviceId:String)

}