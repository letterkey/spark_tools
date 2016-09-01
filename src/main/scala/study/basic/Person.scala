package study.basic

/**
 * Created by Administrator on 2015/7/11.
 */
class Person(val name: String, val age : Int, var myIdioSync: List[String] = List()) {
  val MyName = "MyName"
  val MyAge = "MyAge"

  val myInterests =myIdioSync.mkString(", ")

  override def toString : String = f"My name is $name and I am $age. I have $myInterests as interests"

  def toJson : String = f"""{ "$MyName" : "$name" , "$MyAge": $age }"""
}
