package study.sql.join

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by didi on 17/12/20.
  */
object JoinTestCast extends App{


  val  sparkSession = SparkSession.builder().appName("Sort-merge join test")
      .master("local[*]")
      .config("spark.sql.join.preferSortMergeJoin", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "1")
      .config("spark.sql.defaultSizeInBytes", "100000")
      .getOrCreate()

    val schemaUser = StructType(
      Seq(StructField("userId", IntegerType), StructField("name", StringType))
    )

  val schemaGen = StructType(
    Seq(StructField("uId", IntegerType), StructField("GenName", StringType))
  )
    val schemaOrder = StructType(
      Seq(StructField("id", IntegerType), StructField("customers_id", IntegerType), StructField("amount", DoubleType))
    )

    val ordersRdd = sparkSession.sparkContext.parallelize(Seq(
      (1, 1, 19.5d), (2, 1, 200d),
      (3, 2, 500d), (11, 5,1000d),
      (5, 1, 19.5d), (6, 1, 200d),
      (7, 2, 507d), (8, 11, 1000d)
    )).map(o => Row(o._1,o._2,o._3))

    val customerRdd = sparkSession.sparkContext.parallelize(Seq(
      (1, "用户1"),
      (2, "用户2"),
      (3, "用户3")
    )).map(c => Row(c._1,c._2))

  val userGen = sparkSession.sparkContext.parallelize(Seq(
    (1, "男"),
    (2, "女")
  )).map(c => Row(c._1,c._2))

  val orderDF = sparkSession.createDataFrame(ordersRdd, schemaOrder)

  val customerDF = sparkSession.createDataFrame(customerRdd,schemaUser)
  val genDF = sparkSession.createDataFrame(userGen,schemaGen)
    import sparkSession.implicits._
    val all_data = orderDF.join(customerDF , $"customers_id" === $"userId","left_outer").join(genDF,$"customers_id" === $"uId","left_outer")
    all_data.show(10,false)

    sparkSession.stop()
}
