/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package study.sql

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object HttpWash {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      sparkConf.setAppName("HdfsTest").setMaster("local")
//    sparkConf.setMaster("spark://192.168.56.102:7077")
//    sparkConf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(sparkConf);
    val file = sc.textFile("hdfs://localhost:9000/tmp/log.1420524375192_sample");

    val sort = file.map(line => {
      val list = line.split("\t")
      val m = (list(0),line)
      m
    }).sortByKey(false,1).map(m=>m._1)
//    val reduce = file.flatMap(line => line.split("")).map(word=> (word,1)).reduceByKey(_ + _,1)
////    println(reduce.count())
    sort.saveAsTextFile("/usr/local/result")

    sc.stop()
  }
}
