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

import scala.jdk.CollectionConverters._

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
