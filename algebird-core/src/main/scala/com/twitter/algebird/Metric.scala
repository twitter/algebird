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

import scala.annotation.implicitNotFound

/**
 * A Metric[V] m is a function (V, V) => Double that satisfies the following properties:
 *
 *   1. m(v1, v2) >= 0 2. m(v1, v2) == 0 iff v1 == v2 3. m(v1, v2) == m(v2, v1) 4. m(v1, v3) <= m(v1, v2) +
 *      m(v2, v3)
 *
 * If you implement this trait, make sure that you follow these rules.
 */
object Metric {
  def apply[V: Metric](v1: V, v2: V): Double =
    implicitly[Metric[V]].apply(v1, v2)
  def norm[V: Metric: Monoid](v: V): Double = apply(v, Monoid.zero[V])
  def from[V](f: (V, V) => Double): Metric[V] = new Metric[V] {
    override def apply(v1: V, v2: V): Double = f(v1, v2)
  }

  // See http://en.wikipedia.org/wiki/Minkowski_distance
  def minkowskiIterable[V: Monoid: Metric](p: Double): Metric[Iterable[V]] =
    Metric.from { (a: Iterable[V], b: Iterable[V]) =>
      // TODO: copied from IndexedSeq.scala
      // We need them to be the same length:
      val maxSize = scala.math.max(a.size, b.size)
      def pad(v: Iterable[V]) = {
        val diff = maxSize - v.size
        if (diff > 0) {
          v ++ (Iterator.fill(diff)(Monoid.zero[V]))
        } else {
          v
        }
      }

      val outP = pad(a).view
        .zip(pad(b))
        .map { case (i, j) =>
          math.pow(implicitly[Metric[V]].apply(i, j), p)
        }
        .sum
      math.pow(outP, 1.0 / p)
    }

  def L1Iterable[V: Monoid: Metric]: Metric[Iterable[V]] = minkowskiIterable[V](1.0)
  def L2Iterable[V: Monoid: Metric]: Metric[Iterable[V]] = minkowskiIterable[V](2.0)
  // TODO: Implement Linf, using an ordering on V

  def minkowskiMap[K, V: Monoid: Metric](p: Double): Metric[Map[K, V]] =
    Metric.from { (a: Map[K, V], b: Map[K, V]) =>
      val outP = (a.keySet ++ b.keySet).map { (key: K) =>
        val v1 = a.getOrElse(key, Monoid.zero[V])
        val v2 = b.getOrElse(key, Monoid.zero[V])
        math.pow(implicitly[Metric[V]].apply(v1, v2), p)
      }.sum
      math.pow(outP, 1.0 / p)
    }

  def L1Map[K, V: Monoid: Metric]: Metric[Map[K, V]] = minkowskiMap[K, V](1.0)
  def L2Map[K, V: Monoid: Metric]: Metric[Map[K, V]] = minkowskiMap[K, V](2.0)

  // Implicit values
  implicit val doubleMetric: Metric[Double] =
    Metric.from((a: Double, b: Double) => math.abs(a - b))
  implicit val intMetric: Metric[Int] =
    Metric.from((a: Int, b: Int) => math.abs((a - b).toDouble))
  implicit val longMetric: Metric[Long] =
    Metric.from((a: Long, b: Long) => math.abs((a - b).toDouble))
  implicit val floatMetric: Metric[Float] =
    Metric.from((a: Float, b: Float) => math.abs(a.toDouble - b.toDouble))
  implicit val shortMetric: Metric[Short] =
    Metric.from((a: Short, b: Short) => math.abs((a - b).toDouble))
  implicit val boolMetric: Metric[Boolean] =
    Metric.from((x: Boolean, y: Boolean) => if (x ^ y) 1.0 else 0.0)
  implicit val jDoubleMetric: Metric[JDouble] =
    Metric.from((a: JDouble, b: JDouble) => math.abs(a - b))
  implicit val jIntMetric: Metric[Integer] =
    Metric.from((a: JInt, b: JInt) => math.abs((a - b).toDouble))
  implicit val jLongMetric: Metric[JLong] =
    Metric.from((a: JLong, b: JLong) => math.abs((a - b).toDouble))
  implicit val jFloatMetric: Metric[JFloat] =
    Metric.from((a: JFloat, b: JFloat) => math.abs(a.toDouble - b.toDouble))
  implicit val jShortMetric: Metric[JShort] =
    Metric.from((a: JShort, b: JShort) => math.abs((a - b).toDouble))
  implicit val jBoolMetric: Metric[JBool] =
    Metric.from((x: JBool, y: JBool) => if (x ^ y) 1.0 else 0.0)

  // If you don't want to use L2 as your default metrics, you need to override these
  implicit def iterableMetric[V: Monoid: Metric]: Metric[Iterable[V]] = L2Iterable[V]
  implicit def mapMetric[K, V: Monoid: Metric]: Metric[Map[K, V]] = L2Map[K, V]
}

@implicitNotFound(msg = "Cannot find Metric type class for ${V}")
trait Metric[@specialized(Int, Long, Float, Double) -V] extends java.io.Serializable {
  def apply(v1: V, v2: V): Double
}
