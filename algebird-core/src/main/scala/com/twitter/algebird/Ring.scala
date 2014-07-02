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
 */

@implicitNotFound(msg = "Cannot find Ring type class for ${T}")
trait Ring[@specialized(Int, Long, Float, Double) T] extends Group[T] {
  def one: T // Multiplicative identity
  def times(l: T, r: T): T
  // Left product: (((a * b) * c) * d)
  def product(iter: TraversableOnce[T]): T = Ring.product(iter)(this)
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

object BigIntRing extends NumericRing[BigInt]

object Ring extends GeneratedRingImplicits with ProductRings {
  // This pattern is really useful for typeclasses
  def one[T](implicit rng: Ring[T]) = rng.one
  def times[T](l: T, r: T)(implicit rng: Ring[T]) = rng.times(l, r)
  def asTimesMonoid[T](implicit ring: Ring[T]): Monoid[T] =
    Monoid.from[T](ring.one)(ring.times _)
  // Left product: (((a * b) * c) * d)
  def product[T](iter: TraversableOnce[T])(implicit ring: Ring[T]) = {
    // avoid touching one unless we need to (some items are pseudo-rings)
    if (iter.isEmpty) ring.one
    else iter.reduceLeft(ring.times _)
  }
  // If the ring doesn't have a one, or you want to distinguish empty cases:
  def productOption[T](it: TraversableOnce[T])(implicit rng: Ring[T]): Option[T] =
    it.reduceLeftOption(rng.times _)

  implicit def numericRing[T: Numeric]: Ring[T] = new NumericRing[T]
  implicit val boolRing: Ring[Boolean] = BooleanField
  implicit val jboolRing: Ring[JBool] = JBoolField
  implicit val intRing: Ring[Int] = IntRing
  implicit val jintRing: Ring[JInt] = JIntRing
  implicit val shortRing: Ring[Short] = ShortRing
  implicit val jshortRing: Ring[JShort] = JShortRing
  implicit val longRing: Ring[Long] = LongRing
  implicit val bigIntRing: Ring[BigInt] = BigIntRing
  implicit val jlongRing: Ring[JLong] = JLongRing
  implicit val floatRing: Ring[Float] = FloatField
  implicit val jfloatRing: Ring[JFloat] = JFloatField
  implicit val doubleRing: Ring[Double] = DoubleField
  implicit val jdoubleRing: Ring[JDouble] = JDoubleField
  implicit def indexedSeqRing[T: Ring]: Ring[IndexedSeq[T]] = new IndexedSeqRing[T]
  implicit def mapRing[K, V](implicit ring: Ring[V]) = new MapRing[K, V]()(ring)
  implicit def scMapRing[K, V](implicit ring: Ring[V]) = new ScMapRing[K, V]()(ring)
}
