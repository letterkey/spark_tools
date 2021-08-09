package study

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, input_file_name, regexp_replace, size, split}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, _}
object SparkJson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkJson").master("local[2]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
    val insuer_schema =
      StructType(Array(
          StructField("path", StringType) ,
          StructField("name", StringType),
          StructField("meta",
          StructType(
            Array(
              StructField("title",StringType),
                StructField("dataPageId",StringType)
            )
          )),
        StructField("children",
          ArrayType(
              StructType(
                Array(
                  StructField("path",StringType),
                  StructField("name",StringType)
              )))
        )

      )

    )
    val base_data = spark.read
      .schema(insuer_schema)
      .json("/Users/shuidi/Downloads/path/")
      .withColumn("file_name",split(input_file_name(),"\\/")(size(split(input_file_name(),"\\/"))-1))
//      .printSchema()
      .withColumn("file_name",split(col("file_name"),"\\.")(0))
      .withColumn("title",col("meta.title"))
      .withColumn("dataPageId",col("meta.dataPageId"))

    val base_data_site = base_data.where("children is null")
      .withColumn("children_path",lit(""))
      .withColumn("children_name",lit(""))
      .drop("children")
    val base_data_site_array = base_data.where("children is not null")
      .withColumn("children",explode(col("children")))
      .withColumn("children_path",col("children.path"))
      .withColumn("children_name",col("children.name"))
      .drop("children")
    val union_data = base_data_site.union(base_data_site_array)
      .withColumn("path",concat_ws("/",col("file_name"),col("path"),col("children_path")))
//      .withColumn("children_path",col("children.path"))
//      .withColumn("children_name",col("children.name"))
      .select("dataPageId","path","name","title","file_name")

    union_data.show(1000,false)
    println(union_data.count())
    union_data.repartition(1).write.csv("/Users/shuidi/Downloads/path/new/")

  }

  /**
    * "/Users/shuidi/Downloads/agent.txt"
    * @param spark
    */
  def read_txt(spark:SparkSession, path:String): Unit ={
    val base_data = spark.read.text(path)
      .withColumn("id",split(col("value"),"\t")(0))
      .withColumn("biz",split(col("value"),"\t")(1))
      .withColumn("terminal",split(col("value"),"\t")(2))
      .withColumn("page_id",split(col("value"),"\t")(3))
      .withColumn("page_name",split(col("value"),"\t")(4))
      .withColumn("page_url",split(col("value"),"\t")(5))
      .withColumn("update_user_name",split(col("value"),"\t")(6))
      .withColumn("create_time",split(col("value"),"\t")(7))
      .drop(col("value"))
    //      .toDF("id,biz,terminal,page_id,page_name,page_url,update_user_name,create_time".split(",").map(_.trim):_*)
    //      .createOrReplaceTempView("base_table")
    //      .limit(2)
    base_data.show(10,false)

    base_data
      .withColumn("page_url",explode(split(col("page_url"),"\",\"")))
      .withColumn("page_url",regexp_replace(col("page_url"),"\"|\\[|\\]",""))
      .withColumn("file_path",input_file_name())
      .withColumn("file_name",split(input_file_name(),"\\/")(size(split(input_file_name(),"\\/"))-1))
      .where("id='52'")
      .show(100,false)
    //      .write.csv("/Users/shuidi/Downloads/agent_new.csv")
    //    spark.sqlContext.sql("select url from base_table limit 10") .show(false)

  }
}
