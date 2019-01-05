spark中对rdd增加操作函数的方法：
1.隐式转换：见MuYangFunctions.scala 和TestFunctions.scala
2.自定义RDD :
    a.定义RDD；MuYangRDD
    b.定义工具类：functions,并实现隐式转换修饰；
    MuYangRDD rdd的上游只能是RDD[SalesRecord]类型才能使用隐式转换的函数:discount(discountPercentage:Double)

隐式转换有四种常见的使用场景：
    1.将某一类型转换成预期类型
    2.类型增强与扩展
    3.模拟新的语法
    4.类型类

