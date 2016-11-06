package com.twitter.algebird

import scala.annotation.implicitNotFound

/**
 * Creates an instance of `T` given an `Int`.  We use this type class to generate input data of type `T` for CMS.
 */
@implicitNotFound("No member of type class FromIntLike in scope for ${T}")
trait FromIntLike[T] {
  def fromInt(x: Int): T
}

object FromIntLike {

  implicit object FromIntByte extends FromIntLike[Byte] {
    override def fromInt(x: Int): Byte = x.toByte
  }

  implicit object FromIntShort extends FromIntLike[Short] {
    override def fromInt(x: Int): Short = x.toShort
  }

  implicit object FromIntInt extends FromIntLike[Int] {
    override def fromInt(x: Int): Int = x
  }

  implicit object FromIntLong extends FromIntLike[Long] {
    override def fromInt(x: Int): Long = x.toLong
  }

  implicit object FromIntBytes extends FromIntLike[Bytes] {
    override def fromInt(x: Int): Bytes = Bytes(BigInt(x).toByteArray)
  }

  implicit object FromIntBigInt extends FromIntLike[BigInt] {
    override def fromInt(x: Int): BigInt = BigInt(x)
  }

  implicit object FromIntBigDecimal extends FromIntLike[BigDecimal] {
    override def fromInt(x: Int): BigDecimal = BigDecimal(x)
  }

  implicit object FromIntString extends FromIntLike[String] {
    override def fromInt(x: Int): String = x.toString
  }

}