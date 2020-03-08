package com.twitter.algebird

import java.lang.{Float => JFloat, Double => JDouble}

/**
 * This is here to ease transition to using algebra.Field as the field
 * type. Intended use is to do:
 *
 * {code} import com.twitter.algebird.field._ {/code}
 *
 * Note, this are not strictly lawful since floating point
 * arithmetic using IEEE-754 is only approximately associative
 * and distributive.
 */
object field {
  implicit object ForFloat extends Field[Float] {
    override def one: Float = 1.0f
    override def zero: Float = 0.0f
    override def negate(v: Float): Float = -v
    override def plus(l: Float, r: Float): Float = l + r
    override def sum(v: TraversableOnce[Float]): Float = {
      var sum = 0.0f
      v.foreach(sum += _)
      return sum
    }
    override def trySum(v: TraversableOnce[Float]): Option[Float] =
      if (v.isEmpty) None else Some(sum(v))
    override def minus(l: Float, r: Float): Float = l - r
    override def times(l: Float, r: Float): Float = l * r
    override def div(l: Float, r: Float): Float = l / r
  }
  implicit object ForJFloat extends Field[JFloat] {
    override val one: JFloat = JFloat.valueOf(1.0f)
    override val zero: JFloat = JFloat.valueOf(0.0f)
    override def negate(v: JFloat): JFloat = -v
    override def plus(l: JFloat, r: JFloat): JFloat = l + r
    override def sum(v: TraversableOnce[JFloat]): JFloat = {
      var sum = 0.0f
      v.foreach(sum += _)
      return sum
    }
    override def trySum(v: TraversableOnce[JFloat]): Option[JFloat] =
      if (v.isEmpty) None else Some(sum(v))
    override def minus(l: JFloat, r: JFloat): JFloat = l - r
    override def times(l: JFloat, r: JFloat): JFloat = l * r
    override def div(l: JFloat, r: JFloat): JFloat = l / r
  }
  implicit object ForDouble extends Field[Double] {
    override def one: Double = 1.0
    override def zero: Double = 0.0
    override def negate(v: Double): Double = -v
    override def plus(l: Double, r: Double): Double = l + r
    override def sum(v: TraversableOnce[Double]): Double = {
      var sum = 0.0
      v.foreach(sum += _.floatValue)
      return sum
    }
    override def trySum(v: TraversableOnce[Double]): Option[Double] =
      if (v.isEmpty) None else Some(sum(v))
    override def minus(l: Double, r: Double): Double = l - r
    override def times(l: Double, r: Double): Double = l * r
    override def div(l: Double, r: Double): Double = l / r
  }
  implicit object ForJDouble extends Field[JDouble] {
    override val one: JDouble = JDouble.valueOf(1.0)
    override val zero: JDouble = JDouble.valueOf(0.0)
    override def negate(v: JDouble): JDouble = -v
    override def plus(l: JDouble, r: JDouble): JDouble = l + r
    override def sum(v: TraversableOnce[JDouble]): JDouble = {
      var sum = 0.0
      v.foreach(sum += _.doubleValue)
      return sum
    }
    override def trySum(v: TraversableOnce[JDouble]): Option[JDouble] =
      if (v.isEmpty) None else Some(sum(v))
    override def minus(l: JDouble, r: JDouble): JDouble = l - r
    override def times(l: JDouble, r: JDouble): JDouble = l * r
    override def div(l: JDouble, r: JDouble): JDouble = l / r
  }

  /**
   * These methods were originally on algebird.Field, but are not present on
   * algebra.Field
   */
  implicit class AlgebirdFieldEnrichments[T](val field: Field[T]) extends AnyVal {
    def assertNotZero(t: T): Unit =
      if (t == field.zero)
        throw new IllegalArgumentException(s"found $t, require ${field.zero}")
      else ()

    def nonZeroOption(t: T): Option[T] =
      if (t == field.zero) None
      else Some(t)

    def isNonZero(v: T): Boolean = v != field.zero

    def inverse(t: T): T = field.reciprocal(t)
  }
}
