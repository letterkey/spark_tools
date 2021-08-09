package study

import org.apache.spark.{SparkContext, SparkConf}

/**
 * hello word
 * Created by Administrator on 2014/9/23.
 */
object Count{
  val name = "nm"
  def main(args : Array[String]){
    println(this.getFieldVal(Count,"name"))
    println("++++++++++++++")
    val conf = new SparkConf().setAppName("count")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val result = sc.parallelize(List(1,2,3,4,5,6,7):::List(8,9)).map(_*2)

    val count = result.count()
    val reduce = result.reduce(_+_)
    println(count + "-------------_"+ reduce)
    val a = Array(1,3,5,7,9,1,3,5,7,9)
    val k=10
    getSumInArray(a,k)
  }

  import java.util

  def getSumInArray(base_array: Array[Int], sum: Int): Unit = {
    util.Arrays.sort(base_array)
    var low = 0
    var high = base_array.length - 1
    import scala.collection.mutable.Set
    val result = Set[String]()
    while ( {
      low < high
    }) if (base_array(low) + base_array(high) < sum) low += 1
    else if (base_array(low) + base_array(high) > sum) high -= 1
    else {
      result.add(base_array(low).toString + "," + base_array(high).toString)
      println("组合不去重开始----------------")
      System.out.println(base_array(low) + "," + base_array(high))

      low += 1
      high -= 1
    }

    println("组合不去重结束----------------")


    println("组合重开始----------------")
    result.map(println(_))
    println("组合不去重结束----------------")
  }

  import scala.util.{Failure, Success, Try}
  def getFieldVal(o: Any, name: String): Any = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }
}
