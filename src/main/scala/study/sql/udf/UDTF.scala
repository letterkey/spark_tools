package study.sql.udf

import org.apache.spark.sql.SparkSession

/**
  * Created by yinmuyang on 18/8/7.
  */
object UDTF {
    def main(args: Array[String]) {

        val sqlContext = SparkSession.builder().master("local").getOrCreate().sqlContext;
        val sparkContext = sqlContext.sparkContext
        val a = sparkContext.parallelize(Array((Array("1","fruit"), "apple,banana,pear,jwb"), (Array("2","animal"), "pig,cat,dog,tiger")))
        val b = a.flatMapValues(_.split(",")).map(ele=>{
            val num = ele._1(0)
            val name = ele._1(1)
            val cate = ele._2
            (num,name,cate)
        })
        import sqlContext.implicits._
        b.toDF("num","name","cate").show()
    }
}
