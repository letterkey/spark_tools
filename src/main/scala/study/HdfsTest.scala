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

package study

import org.apache.spark.{Partition, SparkContext, SparkConf}
import org.apache.spark.SparkContext._


object HdfsTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("HdfsTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("hdfs://10.128.17.21:9000/test/CHANGES.txt");


    val reduce = file.flatMap(line => line.split(" ")).map(word=> (word,1)).reduceByKey(_ + _,1)
//    println(reduce.count())

//    reduce.saveAsTextFile("/opt/result")// 保存到本地
    reduce.saveAsTextFile("hdfs://10.128.17.21:9000/out/result") // 保存到hdfs
    sc.stop()
  }
}
