package language

import java.util.Date

/**
  * scala 声明函数方式
  * Created by YMY on 17/9/19.
  */
object Functions {

  def main(args: Array[String]): Unit = {

    printStrings("Runoob", "Scala", "Python")

    // 高阶函数（Higher-Order Function）就是操作其他函数的函数:
    println( apply( layout, 10) )

    // 偏应用函数：绑定第一个 date 参数，第二个参数使用下划线(_)替换缺失的参数列表，并把这个新的函数值的索引的赋给变量
    logWithDateBound("m1")

    // 柯里化(Currying)指的是将原来接受两个参数的函数变成新的接受一个参数的函数的过程。新的函数返回一个以原有第二个参数为参数的函数

  }

  /**
    * 函数声明
    * @param a
    * @param b
    * @return
    */
  def addInt(a:Int,b:Int):Int ={
    var sum = a + b
    sum
  }

  /**
    * 指定默认参数值
    * @param a
    * @return
    */
  def addInt(a:Int = 5) : Int ={
    0
  }

  /**
    * 柯里化函数：指的是将原来接受两个参数的函数变成新的接受一个参数的函数的过程。新的函数返回一个以原有第二个参数为参数的函数
    * @param x
    * @param y
    * @return
    */
  def add(x:Int)(y:Int) = x + y

  /**
    * 设置可变参数
    * @param args
    * @return
    */
  def printStrings(args: String*) : Unit = {
    for(arg : String <- args){
      println(arg)
    }
  }

  // 函数 f 和 值 v 作为参数，而函数 f 又调用了参数 v
  def apply(f: Int => String, v: Int) = f(v)


  def layout[A](x: A) = "[" + x.toString() + "]"

  /**
    * 匿名函数：箭头左边是参数列表，右边是函数体
    */
  var inc = (x: Int) => x + 1
  /**
    * 匿名函数：无参匿名函数
    */
  var userDir = () => System.getProperty("user.dir")


  def log(date : Date ,message : String) = {
    println(date + "________" + message)
  }

  /**
    * 偏应用化函数
    */
  val logWithDateBound = log(new Date(),_ : String)

}
