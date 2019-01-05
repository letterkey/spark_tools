package study.plugin

/**
  * Created by yinmuyang on 18-12-13 18:04.
  */
trait XY {
  implicit def strToInt(str: String) = str.toInt
}
