package org.ymy

/**
 * Created by root on 15-8-18.
 */
class Implicit {

}
class Test(){

}
class RunTest(test:Test){
  def run:Unit = {
    println("implicit------------->"+test)
  }
}
object Implicit extends App{
  implicit def test2RunTest(test:Test) = new RunTest(test)
  val test = new Test
  test.run
}
