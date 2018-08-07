``隐式转换：隐式转换就是：当Scala编译器进行类型匹配时，如果找不到合适的候选，那么隐式转化提供了另外一种途径来告诉编译器如何将当前的类型转换成预期类型``

1. 将某一类型转换成预期类型
2. 类型增强与扩展
3. 模拟新的语法
4. 类型类

实现形式：
1.“implict def”形式的隐式转换
```scala
/**
  * A demo about scala implicit type conversion.
  * @author Laurence Geng
  */
object ImplicitDefDemo {

    object MyImplicitTypeConversion {
        implicit def strToInt(str: String) = str.toInt
    }

    def main(args: Array[String]) {
        //compile error!
        //val max = math.max("1", 2);
        import MyImplicitTypeConversion.strToInt
        val max = math.max("1", 2);
        println(max)
    }
}
```

2.“隐式类”形式的隐式转换
```scala
/**
  * A demo about scala implicit type conversion.
  * @author Laurence Geng
  */
object ImplicitClassDemo {

    implicit class MyImplicitTypeConversion(val str: String){
         def strToInt = str.toInt
    }
    def main(args: Array[String]) {
        //compile error!
        //val max = math.max("1", 2);
        import com.github.scala.myimplicit.ImplicitDefDemo.MyImplicitTypeConversion._
        val max = math.max("1", 2);
        println(max)
    }
}
```

```text
1. 记住隐式转换函数的同一个scop中不能存在参数和返回值完全相同的2个implicit函数。
2. 隐式转换函数只在意 输入类型，返回类型。
3. 隐式转换是scala的语法灵活和简洁的重要组成部分
```