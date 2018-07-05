package study.impli

import java.io.File

import study.impli.ImplicitHelper._


/**
  * Created by YMY on 18/4/9.
  */
object ScalaDemo {
  def main(args: Array[String]): Unit = {
    println(1.add(2))
    //import com.donews.localspark.ImplicitHelper.strToInt 源码一般放在上面
    println(strToInt("1"))
    println(math.max("1", 2))

    echo("hello")("word")
//    import study.impli.ImplVal.name
//    echo("hello")
    //或者像下面这样
    implicit val impl = "implicit"

    echo("hello")
    import Context.file2RichFile
    val file = new File("./README.txt")

    println(file.read)
  }
}
