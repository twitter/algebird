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

import java.lang.{ Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool }

import scala.annotation.implicitNotFound
/**
 * Ring: Group + multiplication (see: http://en.wikipedia.org/wiki/Ring_%28mathematics%29)
 *  and the three elements it defines:
 *  - additive identity aka zero
 *  - addition
 *  - multiplication
 *
 *
 *  Note, if you have distributive property, additive inverses, and multiplicative identity you
 *  can prove you have a commutative group under the ring:
 *
 *  1. (a + 1)*(b + 1) = a(b + 1) + (b + 1)
 *  2.                 = ab + a + b + 1
 *  3. or:
 *  4.
 *  5.                 = (a + 1)b + (a + 1)
 *  6.                 = ab + b + a + 1
 *  7.
 *  8. So: ab + a + b + 1 == ab + b + a + 1
 *  9.   using the fact that -(ab) and -1 exist, we get:
 * 10. a + b == b + a
 */

@implicitNotFound(msg = "Cannot find Ring type class for ${T}")
trait Ring[@specialized(Int, Long, Float, Double) T] extends Group[T] {
  def one: T
  def times(a: T, b: T): T
  def product(iter: TraversableOnce[T]): T =
    if (iter.isEmpty) one // avoid hitting one as some have abused Ring for Rng
    else iter.reduce(times)
}

// For Java interop so they get the default methods
abstract class AbstractRing[T] extends Ring[T]

class NumericRing[T](implicit num: Numeric[T]) extends Ring[T] {
  override def zero = num.zero
  override def one = num.one
  override def negate(t: T) = num.negate(t)
  override def plus(l: T, r: T) = num.plus(l, r)
  override def minus(l: T, r: T) = num.minus(l, r)
  override def times(l: T, r: T) = num.times(l, r)
}

object IntRing extends Ring[Int] {
  override def zero = 0
  override def one = 1
  override def negate(v: Int) = -v
  override def plus(l: Int, r: Int) = l + r
  override def minus(l: Int, r: Int) = l - r
  override def times(l: Int, r: Int) = l * r
}

object ShortRing extends Ring[Short] {
  override def zero = 0.toShort
  override def one = 1.toShort
  override def negate(v: Short) = (-v).toShort
  override def plus(l: Short, r: Short) = (l + r).toShort
  override def minus(l: Short, r: Short) = (l - r).toShort
  override def times(l: Short, r: Short) = (l * r).toShort
}

object LongRing extends Ring[Long] {
  override def zero = 0L
  override def one = 1L
  override def negate(v: Long) = -v
  override def plus(l: Long, r: Long) = l + r
  override def minus(l: Long, r: Long) = l - r
  override def times(l: Long, r: Long) = l * r
}

object FloatRing extends Ring[Float] {
  override def one = 1.0f
  override def zero = 0.0f
  override def negate(v: Float) = -v
  override def plus(l: Float, r: Float) = l + r
  override def minus(l: Float, r: Float) = l - r
  override def times(l: Float, r: Float) = l * r
}

object DoubleRing extends Ring[Double] {
  override def one = 1.0
  override def zero = 0.0
  override def negate(v: Double) = -v
  override def plus(l: Double, r: Double) = l + r
  override def minus(l: Double, r: Double) = l - r
  override def times(l: Double, r: Double) = l * r
}

object BooleanRing extends Ring[Boolean] {
  override def one = true
  override def zero = false
  override def negate(v: Boolean) = v
  override def plus(l: Boolean, r: Boolean) = l ^ r
  override def minus(l: Boolean, r: Boolean) = l ^ r
  override def times(l: Boolean, r: Boolean) = l && r
}

object BigIntRing extends NumericRing[BigInt]

trait NumericRingProvider {
  implicit def numericRing[T: Numeric]: Ring[T] = new NumericRing[T]
}

object Ring extends GeneratedRingImplicits with ProductRings {
  // This pattern is really useful for typeclasses
  def one[T](implicit rng: Ring[T]) = rng.one
  def times[T](l: T, r: T)(implicit rng: Ring[T]) = rng.times(l, r)
  def asTimesMonoid[T](implicit ring: Ring[T]): Monoid[T] =
    Monoid.from[T](ring.one)(ring.times _)
  // Left product: (((a * b) * c) * d)
  def product[T](iter: TraversableOnce[T])(implicit ring: Ring[T]) =
    ring.product(iter)

  // If the ring doesn't have a one, or you want to distinguish empty cases:
  def productOption[T](it: TraversableOnce[T])(implicit rng: Ring[T]): Option[T] =
    if (it.isEmpty) None
    else Some(rng.product(it))

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
  implicit def indexedSeqRing[T: Ring]: Ring[IndexedSeq[T]] = new IndexedSeqRing[T]
  implicit def mapRing[K, V](implicit ring: Ring[V]) = new MapRing[K, V]()(ring)
  implicit def scMapRing[K, V](implicit ring: Ring[V]) = new ScMapRing[K, V]()(ring)
}
