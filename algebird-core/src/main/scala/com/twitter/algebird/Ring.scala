/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.twitter.algebird

import java.lang.{
  Boolean => JBool,
  Double => JDouble,
  Float => JFloat,
  Integer => JInt,
  Long => JLong,
  Short => JShort
}
import algebra.ring.{Rig, Ring => ARing, Rng}
import algebra.CommutativeGroup

import scala.annotation.implicitNotFound

/**
 * Ring: Group + multiplication (see: http://en.wikipedia.org/wiki/Ring_%28mathematics%29) and the three
 * elements it defines:
 *   - additive identity aka zero
 *   - addition
 *   - multiplication
 *
 * Note, if you have distributive property, additive inverses, and multiplicative identity you can prove you
 * have a commutative group under the ring:
 *
 *   1. (a + 1)*(b + 1) = a(b + 1) + (b + 1) 2. = ab + a + b + 1 3. or: 4. 5. = (a + 1)b + (a + 1) 6. = ab + b
 *      + a + 1 7. 8. So: ab + a + b + 1 == ab + b + a + 1 9. using the fact that -(ab) and -1 exist, we get:
 *      10. a + b == b + a
 */

@implicitNotFound(msg = "Cannot find Ring type class for ${T}")
trait Ring[@specialized(Int, Long, Float, Double) T] extends Group[T] with CommutativeGroup[T] with ARing[T] {
  override def one: T
  override def times(a: T, b: T): T
  override def product(iter: TraversableOnce[T]): T =
    if (iter.isEmpty) one // avoid hitting one as some have abused Ring for Rng
    else iter.reduce(times)
}

// For Java interop so they get the default methods
abstract class AbstractRing[T] extends Ring[T]

class ConstantRing[T](constant: T) extends ConstantGroup[T](constant) with Ring[T] {
  override def one: T = constant
  override def times(a: T, b: T): T = constant
}

class NumericRing[T](implicit num: Numeric[T]) extends Ring[T] {
  override def zero: T = num.zero
  override def one: T = num.one
  override def negate(t: T): T = num.negate(t)
  override def plus(l: T, r: T): T = num.plus(l, r)
  override def minus(l: T, r: T): T = num.minus(l, r)
  override def times(l: T, r: T): T = num.times(l, r)
}

object IntRing extends Ring[Int] {
  override def zero: Int = 0
  override def one: Int = 1
  override def negate(v: Int): Int = -v
  override def plus(l: Int, r: Int): Int = l + r
  override def minus(l: Int, r: Int): Int = l - r
  override def times(l: Int, r: Int): Int = l * r
  override def sum(t: TraversableOnce[Int]): Int = {
    var sum = 0
    val iter = t.toIterator
    while (iter.hasNext) {
      sum += iter.next()
    }
    sum
  }
  override def sumOption(t: TraversableOnce[Int]): Option[Int] =
    if (t.isEmpty) None
    else Some(sum(t))
}

object ShortRing extends Ring[Short] {
  override def zero: Short = 0.toShort
  override def one: Short = 1.toShort
  override def negate(v: Short): Short = (-v).toShort
  override def plus(l: Short, r: Short): Short = (l + r).toShort
  override def minus(l: Short, r: Short): Short = (l - r).toShort
  override def times(l: Short, r: Short): Short = (l * r).toShort
  override def sum(t: TraversableOnce[Short]): Short = {
    var sum = 0
    val iter = t.toIterator
    while (iter.hasNext) {
      sum += iter.next()
    }
    sum.toShort
  }
  override def sumOption(t: TraversableOnce[Short]): Option[Short] =
    if (t.isEmpty) None
    else Some(sum(t))
}

object LongRing extends Ring[Long] {
  override def zero: Long = 0L
  override def one: Long = 1L
  override def negate(v: Long): Long = -v
  override def plus(l: Long, r: Long): Long = l + r
  override def minus(l: Long, r: Long): Long = l - r
  override def times(l: Long, r: Long): Long = l * r
  override def sum(t: TraversableOnce[Long]): Long = {
    var sum = 0L
    val iter = t.toIterator
    while (iter.hasNext) {
      sum += iter.next()
    }
    sum
  }
  override def sumOption(t: TraversableOnce[Long]): Option[Long] =
    if (t.isEmpty) None
    else Some(sum(t))
}

object FloatRing extends Ring[Float] {
  override def one: Float = 1.0f
  override def zero: Float = 0.0f
  override def negate(v: Float): Float = -v
  override def plus(l: Float, r: Float): Float = l + r
  override def minus(l: Float, r: Float): Float = l - r
  override def times(l: Float, r: Float): Float = l * r

  // see: https://en.wikipedia.org/wiki/Kahan_summation_algorithm
  // for this algorithm
  override def sumOption(t: TraversableOnce[Float]): Option[Float] =
    if (t.isEmpty) None
    else
      Some {
        val iter = t.toIterator
        var sum = iter.next().toDouble
        var c = 0.0
        while (iter.hasNext) {
          val y = iter.next().toDouble - c
          val t = sum + y
          c = (t - sum) - y
          sum = t
        }

        sum.toFloat
      }
}

object DoubleRing extends Ring[Double] {
  override def one: Double = 1.0
  override def zero: Double = 0.0
  override def negate(v: Double): Double = -v
  override def plus(l: Double, r: Double): Double = l + r
  override def minus(l: Double, r: Double): Double = l - r
  override def times(l: Double, r: Double): Double = l * r

  // see: https://en.wikipedia.org/wiki/Kahan_summation_algorithm
  // for this algorithm
  override def sumOption(t: TraversableOnce[Double]): Option[Double] =
    if (t.isEmpty) None
    else
      Some {
        val iter = t.toIterator
        var sum = iter.next()
        var c = 0.0
        while (iter.hasNext) {
          val y = iter.next() - c
          val t = sum + y
          c = (t - sum) - y
          sum = t
        }

        sum
      }
}

object BooleanRing extends Ring[Boolean] {
  override def one: Boolean = true
  override def zero: Boolean = false
  override def negate(v: Boolean): Boolean = v
  override def plus(l: Boolean, r: Boolean): Boolean = l ^ r
  override def minus(l: Boolean, r: Boolean): Boolean = l ^ r
  override def times(l: Boolean, r: Boolean): Boolean = l && r
}

object BigIntRing extends NumericRing[BigInt]
object BigDecimalRing extends NumericRing[BigDecimal]

trait NumericRingProvider {
  implicit def numericRing[T: Numeric]: Ring[T] = new NumericRing[T]
}

class FromAlgebraRing[T](r: ARing[T]) extends Ring[T] {
  override def zero: T = r.zero
  override def one: T = r.one
  override def plus(a: T, b: T): T = r.plus(a, b)
  override def negate(t: T): T = r.negate(t)
  override def minus(a: T, b: T): T = r.minus(a, b)
  override def sum(ts: TraversableOnce[T]): T = r.sum(ts)
  override def sumOption(ts: TraversableOnce[T]): Option[T] = r.trySum(ts)
  override def times(a: T, b: T): T = r.times(a, b)
  override def product(ts: TraversableOnce[T]): T = r.product(ts)
}

/**
 * In some legacy cases, we have implemented Rings where we lacked the full laws. This allows you to be
 * precise (only implement the structure you have), but unsafely use it as a Ring in legacy code that is
 * expecting a Ring.
 */
class UnsafeFromAlgebraRig[T](r: Rig[T]) extends Ring[T] {
  override def zero: T = r.zero
  override def one: T = r.one
  override def plus(a: T, b: T): T = r.plus(a, b)
  override def negate(t: T): T =
    sys.error(s"$r is a Rig, there is no additive inverse")
  override def minus(a: T, b: T): T =
    sys.error(s"$r is a Rig, there is no additive inverse")
  override def sum(ts: TraversableOnce[T]): T = r.sum(ts)
  override def sumOption(ts: TraversableOnce[T]): Option[T] = r.trySum(ts)
  override def times(a: T, b: T): T = r.times(a, b)
  override def product(ts: TraversableOnce[T]): T = r.product(ts)
}

/**
 * In some legacy cases, we have implemented Rings where we lacked the full laws. This allows you to be
 * precise (only implement the structure you have), but unsafely use it as a Ring in legacy code that is
 * expecting a Ring.
 */
class UnsafeFromAlgebraRng[T](r: Rng[T]) extends Ring[T] {
  override def zero: T = r.zero
  override def one: T =
    sys.error(s"multiplicative identity for Rng $r unimplemented")
  override def plus(a: T, b: T): T = r.plus(a, b)
  override def negate(t: T): T = r.negate(t)
  override def minus(a: T, b: T): T = r.minus(a, b)
  override def sum(ts: TraversableOnce[T]): T = r.sum(ts)
  override def sumOption(ts: TraversableOnce[T]): Option[T] = r.trySum(ts)
  override def times(a: T, b: T): T = r.times(a, b)
  override def product(ts: TraversableOnce[T]): T =
    if (ts.isEmpty) one
    else ts.reduce(r.times) // avoid calling one, since that will throw here
}

private[algebird] trait RingImplicits0 extends NumericRingProvider {
  implicit def fromAlgebraRing[T](implicit r: ARing[T]): Ring[T] =
    new FromAlgebraRing(r)
}

object Ring extends GeneratedRingImplicits with ProductRings with RingImplicits0 {
  // This pattern is really useful for typeclasses
  def one[T](implicit rng: Ring[T]): T = rng.one
  def times[T](l: T, r: T)(implicit rng: Ring[T]): T = rng.times(l, r)
  def asTimesMonoid[T](implicit ring: Ring[T]): Monoid[T] = new Monoid[T] {
    override def zero: T = ring.one
    override def plus(a: T, b: T): T = ring.times(a, b)
    override def sumOption(ts: TraversableOnce[T]): Option[T] =
      if (ts.isEmpty) None
      else Some(ring.product(ts))
    override def sum(ts: TraversableOnce[T]): T =
      ring.product(ts)
  }
  // Left product: (((a * b) * c) * d)
  def product[T](iter: TraversableOnce[T])(implicit ring: Ring[T]): T =
    ring.product(iter)

  // If the ring doesn't have a one, or you want to distinguish empty cases:
  def productOption[T](it: TraversableOnce[T])(implicit rng: Ring[T]): Option[T] =
    if (it.isEmpty) None
    else Some(rng.product(it))

  implicit def unitRing: Ring[Unit] = new ConstantRing(())
  implicit def boolRing: Ring[Boolean] = BooleanRing
  implicit def jboolRing: Ring[JBool] = JBoolRing
  implicit def intRing: Ring[Int] = IntRing
  implicit def jintRing: Ring[JInt] = JIntRing
  implicit def shortRing: Ring[Short] = ShortRing
  implicit def jshortRing: Ring[JShort] = JShortRing
  implicit def longRing: Ring[Long] = LongRing
  implicit def bigIntRing: Ring[BigInt] = BigIntRing
  implicit def jlongRing: Ring[JLong] = JLongRing
  implicit def floatRing: Ring[Float] = FloatRing
  implicit def jfloatRing: Ring[JFloat] = JFloatRing
  implicit def doubleRing: Ring[Double] = DoubleRing
  implicit def jdoubleRing: Ring[JDouble] = JDoubleRing
  implicit def indexedSeqRing[T: Ring]: Ring[IndexedSeq[T]] =
    new IndexedSeqRing[T]

  /**
   * This is actually a Rng but we leave the unsafe Ring for legacy reasons
   */
  implicit def mapRing[K, V](implicit ring: Ring[V]): Ring[Map[K, V]] =
    new UnsafeFromAlgebraRng(new MapRing[K, V]()(ring))

  /**
   * This is actually a Rng but we leave the unsafe Ring for legacy reasons
   */
  implicit def scMapRing[K, V](implicit ring: Ring[V]): Ring[scala.collection.Map[K, V]] =
    new UnsafeFromAlgebraRng(new ScMapRing[K, V]()(ring))
}
