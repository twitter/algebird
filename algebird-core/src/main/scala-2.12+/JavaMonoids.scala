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
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._

object JIntRing extends Ring[JInt] {
  override val zero: JInt = JInt.valueOf(0)
  override val one: JInt = JInt.valueOf(1)
  override def plus(x: JInt, y: JInt): JInt = x + y
  override def negate(x: JInt): JInt = -x
  override def minus(x: JInt, y: JInt): JInt = x - y
  override def times(x: JInt, y: JInt): JInt = x * y
}

object JShortRing extends Ring[JShort] {
  override val zero: JShort = Short.box(0)
  override val one: JShort = Short.box(1)
  override def plus(x: JShort, y: JShort): JShort = (x + y).toShort
  override def negate(x: JShort): JShort = (-x).toShort
  override def minus(x: JShort, y: JShort): JShort = (x - y).toShort
  override def times(x: JShort, y: JShort): JShort = (x * y).toShort
}

object JLongRing extends Ring[JLong] {
  override val zero: JLong = JLong.valueOf(0L)
  override val one: JLong = JLong.valueOf(1L)
  override def plus(x: JLong, y: JLong): JLong = x + y
  override def negate(x: JLong): JLong = -x
  override def minus(x: JLong, y: JLong): JLong = x - y
  override def times(x: JLong, y: JLong): JLong = x * y
}

object JFloatRing extends Ring[JFloat] {
  override val zero: JFloat = JFloat.valueOf(0.0f)
  override val one: JFloat = JFloat.valueOf(1.0f)
  override def plus(x: JFloat, y: JFloat): JFloat = x + y
  override def negate(x: JFloat): JFloat = -x
  override def minus(x: JFloat, y: JFloat): JFloat = x - y
  override def times(x: JFloat, y: JFloat): JFloat = x * y
}

object JDoubleRing extends Ring[JDouble] {
  override val zero: JDouble = JDouble.valueOf(0.0)
  override val one: JDouble = JDouble.valueOf(1.0)
  override def plus(x: JDouble, y: JDouble): JDouble = x + y
  override def negate(x: JDouble): JDouble = -x
  override def minus(x: JDouble, y: JDouble): JDouble = x - y
  override def times(x: JDouble, y: JDouble): JDouble = x * y
}

object JBoolRing extends Ring[JBool] {
  override val zero: JBool = JBool.FALSE
  override val one: JBool = JBool.TRUE
  override def plus(x: JBool, y: JBool): JBool =
    JBool.valueOf(x.booleanValue ^ y.booleanValue)
  override def negate(x: JBool): JBool = x
  override def minus(x: JBool, y: JBool): JBool = plus(x, y)
  override def times(x: JBool, y: JBool): JBool =
    JBool.valueOf(x.booleanValue & y.booleanValue)
}

/**
 * Since Lists are mutable, this always makes a full copy. Prefer scala immutable Lists if you use scala
 * immutable lists, the tail of the result of plus is always the right argument
 */
class JListMonoid[T] extends Monoid[JList[T]] {
  override def isNonZero(x: JList[T]): Boolean = !x.isEmpty
  override lazy val zero: JArrayList[T] = new JArrayList[T](0)
  override def plus(x: JList[T], y: JList[T]): JArrayList[T] = {
    val res = new JArrayList[T](x.size + y.size)
    res.addAll(x)
    res.addAll(y)
    res
  }
}

/**
 * Since maps are mutable, this always makes a full copy. Prefer scala immutable maps if you use scala
 * immutable maps, this operation is much faster TODO extend this to Group, Ring
 */
class JMapMonoid[K, V: Semigroup] extends Monoid[JMap[K, V]] {
  override lazy val zero: JHashMap[K, V] = new JHashMap[K, V](0)

  val nonZero: (V => Boolean) = implicitly[Semigroup[V]] match {
    case mon: Monoid[?] => mon.isNonZero(_)
    case _              => _ => true
  }

  override def isNonZero(x: JMap[K, V]): Boolean =
    !x.isEmpty && (implicitly[Semigroup[V]] match {
      case mon: Monoid[?] =>
        x.values.asScala.exists(v => mon.isNonZero(v))
      case _ => true
    })
  override def plus(x: JMap[K, V], y: JMap[K, V]): JHashMap[K, V] = {
    val (big, small, bigOnLeft) =
      if (x.size > y.size) {
        (x, y, true)
      } else {
        (y, x, false)
      }
    val vsemi = implicitly[Semigroup[V]]
    val result = new JHashMap[K, V](big.size + small.size)
    result.putAll(big)
    small.entrySet.asScala.foreach { kv =>
      val smallK = kv.getKey
      val smallV = kv.getValue
      if (big.containsKey(smallK)) {
        val bigV = big.get(smallK)
        val newV =
          if (bigOnLeft) vsemi.plus(bigV, smallV) else vsemi.plus(smallV, bigV)
        if (nonZero(newV))
          result.put(smallK, newV)
        else
          result.remove(smallK)
      } else {
        // No need to explicitly add with zero on V, just put in the small value
        result.put(smallK, smallV)
      }
    }
    result
  }
}
