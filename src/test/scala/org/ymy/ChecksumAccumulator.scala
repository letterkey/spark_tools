package org.ymy

import scala.collection.mutable.Map
/**
 * 1.伴生类和伴生对象：伴生对象的单例作用,伴生对象不能不可以带参数
 * 2. 方法定义方式：返回值和无返回值
 */
class ChecksumAccumulator {
  private var sum = 0
  def add(b: Byte) {sum += b} // 此种def方法返回Unit，“=”可以省略
  def checksum(): Int = ~(sum & 0xFF) + 1 // 此种方法要返回一个Int类型的值，不能省略“=”号

}

object ChecksumAccumulator {
  private val cache = Map[String, Int]()

  def calculate(s: String): Int =
    if (cache.contains(s)){
      cache(s)
    }else {
      val acc = new ChecksumAccumulator
      for(c <- s)
        acc.add(c.toByte)

      val cs = acc.checksum()
      cache += (s -> cs)
      cs
    }
}