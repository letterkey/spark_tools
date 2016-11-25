package study.basic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import study.rdd.MongoRDD

/**
 *rdd的基本转换
 */
object ReadMongo {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf()
     sparkConf.setAppName("wordCount").setMaster("local")
     val sc = new SparkContext(sparkConf)
       val sql = new SQLContext(sc)
       val mongoUrl = "mongodb://192.168.100.182:27017"
       val database = "workout"
       val collection = "courses"
       val query = new Document()
       val partitions = 4
       val mongoRdd = new MongoRDD(sc,mongoUrl,database,collection,query,partitions)
       mongoRdd.foreach(println(_))
   }
 }
