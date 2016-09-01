package org.ymy

/**
 * 带参数的类
 */
class Rational(n: Int, d: Int) extends Ordered[Rational]{

  require(d != 0) // 检查参数d的合法性
  private val g = gcd(n.abs, d.abs)
  val numer: Int = n / g
  val denom: Int = d / g
  // 辅助构造函数：辅助构造函数
  def this(i: Int) = this(i, 1)
  def this(d: Double) = this(d.toInt)
  // 必须使用override关键字标示重载父类方法
  override def toString = {
    val den = if(denom != 1) "/" + denom else ""
    numer + den
  }

  private def gcd(a: Int, b: Int): Int =
    if (b == 0) a else gcd(b, a % b)

  def +(that: Rational): Rational = {
    new Rational(
      numer * that.denom + denom * that.numer,
      denom * that.denom
    )
  }

  def *(that: Rational): Rational = {
    new Rational(
      numer * that.numer,
      denom * that.denom
    )
  }

  def /(that: Rational): Rational = {
    new Rational(
      numer * that.denom,
      denom * that.numer
    )
  }
  // 方法的重载
  def /(that: Int): Rational = {
    new Rational(
      numer,
      denom * that
    )
  }

  def -(that: Rational): Rational = {
    new Rational(
      numer * that.denom - denom * that.numer,
      denom * that.denom
    )
  }

  def +(that: Int): Rational = {
    new Rational(
      numer + denom * that,
      denom
    )
  }

  def *(that: Int): Rational = {
    new Rational(
      numer * that,
      denom
    )
  }


  def -(that: Int): Rational = {
    new Rational(
      numer - denom * that,
      denom
    )
  }

  def lessThan(that: Rational): Boolean = {
    numer * that.denom < denom * that.numer
  }

  def max(that: Rational): Rational = {
    if(this.lessThan(that)) that else this
  }

  def compare(that: Rational) =
    (numer * that.denom) - (denom * that.numer)

}