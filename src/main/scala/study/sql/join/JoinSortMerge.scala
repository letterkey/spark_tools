package study.sql.join

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
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

    val ordersWithCustomers = ordersDataFrame.join(customersDataFrame, $"customers_id" === $"cid")
    val mergedOrdersWithCustomers = ordersWithCustomers.collect().map(toAssertRowInterval(_))
    val explainedPlan = ordersWithCustomers.queryExecution.toString()

    println(explainedPlan)
    println(explainedPlan.contains("ShuffledHashJoin [customers_id")) //shouldBe true
    println(explainedPlan.contains("SortMergeJoin [customers_id")) //shouldBe false
      println(mergedOrdersWithCustomers.size) //shouldEqual(6)
      mergedOrdersWithCustomers.foreach(println(_))
//    should contain allOf(
//      "1-1:1-19.5-1:1-Customer_1", "2-1:1-200.0-1:1-Customer_1", "5-1:1-19.5-1:1-Customer_1",
//      "6-1:1-200.0-1:1-Customer_1", "3-2:1-500.0-2:1-Customer_2", "7-2:1-500.0-2:1-Customer_2"
//    )

    sparkSession.stop()
  }

  private def toAssertRowInterval(row: Row): String = {
    val orderId = row.getInt(0)
    val orderCustomerId = row.getAs[CalendarInterval](1)
    val orderAmount = row.getDouble(2)
    val customerId = row.getAs[CalendarInterval](3)
    val customerLogin = row.getString(4)
    s"${orderId}-${orderCustomerId.months}:${orderCustomerId.milliseconds()}-"+
    s"${orderAmount}-${customerId.months}:${customerId.milliseconds()}-${customerLogin}"
  }

    private def toAssertRow(row: Row): String = {
    val orderId = row.getInt(0)
    val orderCustomerId = row.getInt(1)
    val orderAmount = row.getDouble(2)
    val customerId = row.getInt(3)
    val customerLogin = row.getString(4)
    s"${orderId}-${orderCustomerId}-${orderAmount}-${customerId}-${customerLogin}"
  }
}
