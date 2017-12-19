package study.sql.join

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
  * sort merge join方式排序后同一个partition中做join，注意join的key是可以sortable的
  * Created by YMY on 17/12/18.
  */
object JoinSortMerge {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Sort-merge join test")
      .master("local[*]")
      .config("spark.sql.join.preferSortMergeJoin", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "1")
      .config("spark.sql.defaultSizeInBytes", "100000")
      .getOrCreate()

    import sparkSession.implicits._
    // 加载key可以sort的dataframe
    val tup_data = getSortableDataSet(sparkSession)
//    val tup_data = getDataSet(sparkSession) // key is not sortable

    val customersDataFrame = tup_data._1
    val ordersDataFrame = tup_data._2
    val ordersWithCustomers = ordersDataFrame.join(customersDataFrame, $"customers_id" === $"cid")
    val mergedOrdersWithCustomers = ordersWithCustomers.collect()
    val explainedPlan = ordersWithCustomers.queryExecution.toString()

    println(explainedPlan)
    println(explainedPlan.contains("ShuffledHashJoin [customers_id")) //shouldBe true
    println(explainedPlan.contains("SortMergeJoin [customers_id")) //shouldBe false
    println(mergedOrdersWithCustomers.size) //shouldEqual(6)
    mergedOrdersWithCustomers.foreach(println(_))

    sparkSession.stop()
  }

  def getSortableDataSet(sparkSession : SparkSession) : Tuple2[DataFrame,DataFrame] ={
    val schema = StructType(
      Seq(StructField("cid", IntegerType), StructField("login", StringType))
    )

    val schemaOrder = StructType(
      Seq(StructField("id", IntegerType), StructField("customers_id", IntegerType), StructField("amount", DoubleType))
    )

    val customersRdd = sparkSession.sparkContext.parallelize((1 to 3).map(nr => (nr, s"Customer_${nr}")))
      .map(attributes => Row(attributes._1, attributes._2))

    val customersDataFrame = sparkSession.createDataFrame(customersRdd, schema)

    val ordersRdd = sparkSession.sparkContext.parallelize(Seq(
      (1, 1, 19.5d), (2, 1, 200d),
      (3, 2, 500d), (11, 5,1000d),
      (5, 1, 19.5d), (6, 1, 200d),
      (7, 2, 500d), (8, 11, 1000d)
    ).map(attributes => Row(attributes._1, attributes._2, attributes._3)))
    val ordersDataFrame = sparkSession.createDataFrame(ordersRdd, schemaOrder)

    (customersDataFrame, ordersDataFrame)
  }

  /**
    * key 不能sort，没有实现sort方法
    * @param sparkSession
    */
  def getDataSet(sparkSession: SparkSession) : Tuple2[DataFrame,DataFrame] ={
    val schema = StructType(
      Seq(StructField("cid", CalendarIntervalType), StructField("login", StringType))
    )

    val schemaOrder = StructType(
      Seq(StructField("id", IntegerType), StructField("customers_id", CalendarIntervalType), StructField("amount", DoubleType))
    )

    val customersRdd = sparkSession.sparkContext.parallelize((1 to 3).map(nr => (new CalendarInterval(nr, 1000), s"Customer_${nr}")))
      .map(attributes => Row(attributes._1, attributes._2))

    val customersDataFrame = sparkSession.createDataFrame(customersRdd, schema)

    val ordersRdd = sparkSession.sparkContext.parallelize(Seq(
      (1, new CalendarInterval(1, 1000), 19.5d), (2, new CalendarInterval(1, 1000), 200d),
      (3, new CalendarInterval(2, 1000), 500d), (4, new CalendarInterval(11, 1000), 1000d),
      (5, new CalendarInterval(1, 1000), 19.5d), (6, new CalendarInterval(1, 1000), 200d),
      (7, new CalendarInterval(2, 1000), 500d), (8, new CalendarInterval(11, 1000), 1000d)
    ).map(attributes => Row(attributes._1, attributes._2, attributes._3)))
    val ordersDataFrame = sparkSession.createDataFrame(ordersRdd, schemaOrder)
    (customersDataFrame, ordersDataFrame)
  }
}
