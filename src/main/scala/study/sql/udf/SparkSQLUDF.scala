package study.sql.udf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * spark sql udf/udaf实例
  * Created by YMY on 17/12/21.
  */
object SparkSQLUDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("SparkSQLWindowFunctionOps").getOrCreate()

    val bigData = Array("Spark","Hadoop","Flink","Spark","Hadoop","Flink","Spark","Hadoop","Flink","Spark","Hadoop","Flink")
    val bigDataRDD = spark.sparkContext.parallelize(bigData)

    val bigDataRowRDD = bigDataRDD.map(line => Row(line))
    val structType = StructType(Array(StructField("name",StringType,true)))
    val bigDataDF = spark.createDataFrame(bigDataRowRDD, structType)

    bigDataDF.registerTempTable("bigDataTable")

    /*
     * 通过HiveContext注册UDF，在scala2.10.x版本UDF函数最多可以接受22个输入参数
     */
    spark.udf.register("computeLength",(input:String) => input.length)
    spark.sql("select name,computeLength(name)  as length from bigDataTable")
      .show

    spark.udf.register("wordCount",new MyUDAF)
    spark.sql("select name,wordCount(name) as count,computeLength(name) as length from bigDataTable group by name ")
      .show
  }
}


/**
  * 用户自定义函数
  */
class MyUDAF extends UserDefinedAggregateFunction
{
  /**
    * 指定具体的输入数据的类型
    * 自段名称随意：Users can choose names to identify the input arguments - 这里可以是“name”，或者其他任意串
    */
  override def inputSchema:StructType = StructType(Array(StructField("name",StringType,true)))

  /**
    * 在进行聚合操作的时候所要处理的数据的中间结果类型
    */
  override def bufferSchema:StructType = StructType(Array(StructField("count",IntegerType,true)))

  /**
    * 返回类型
    */
  override def dataType:DataType = IntegerType

  /**
    * whether given the same input,
    * always return the same output
    * true: yes
    */
  override def deterministic:Boolean = true

  /**
    * Initializes the given aggregation buffer
    */
  override def initialize(buffer:MutableAggregationBuffer):Unit = {buffer(0)=0}

  /**
    * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
    */
  override def update(buffer:MutableAggregationBuffer,input:Row):Unit={
    buffer(0) = buffer.getInt(0)+1
  }

  /**
    * 最后在分布式节点进行local reduce完成后需要进行全局级别的merge操作
    */
  override def merge(buffer1:MutableAggregationBuffer,buffer2:Row):Unit={
    buffer1(0) = buffer1.getInt(0)+buffer2.getInt(0)
  }

  /**
    * 返回UDAF最后的计算结果
    */
  override def evaluate(buffer:Row):Any = buffer.getInt(0)
}