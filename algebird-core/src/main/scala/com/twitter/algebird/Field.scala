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

import scala.annotation.tailrec
import scala.annotation.implicitNotFound

import java.lang.{ Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool }
import java.util.{ List => JList, Map => JMap }

/**
 * Field: Ring + division. It is a generalization of Ring and adds support for inversion and
 *   multiplicative identity.
 */

@implicitNotFound(msg = "Cannot find Field type class for ${T}")
trait Field[@specialized(Int, Long, Float, Double) T] extends Ring[T] {
  // default implementation uses div YOU MUST OVERRIDE ONE OF THESE
  def inverse(v: T): T = {
    assertNotZero(v)
    div(one, v)
  }
  // default implementation uses inverse:
  def div(l: T, r: T): T = {
    assertNotZero(r)
    times(l, inverse(r))
  }
}

// For Java interop so they get the default methods
abstract class AbstractField[T] extends Field[T]

object FloatField extends Field[Float] {
  override def one = 1.0f
  override def zero = 0.0f
  override def negate(v: Float) = -v
  override def plus(l: Float, r: Float) = l + r
  override def minus(l: Float, r: Float) = l - r
  override def times(l: Float, r: Float) = l * r
  override def div(l: Float, r: Float) = {
    assertNotZero(r)
    l / r
  }
  override def sum(t: TraversableOnce[Float]): Float = {
    var sum = 0.0f
    t.foreach(sum += _)
    sum
  }
  override def sumOption(t: TraversableOnce[Float]): Option[Float] =
    if (t.isEmpty) None
    else Some(sum(t))
}

object DoubleField extends Field[Double] {
  override def one = 1.0
  override def zero = 0.0
  override def negate(v: Double) = -v
  override def plus(l: Double, r: Double) = l + r
  override def minus(l: Double, r: Double) = l - r
  override def times(l: Double, r: Double) = l * r
  override def div(l: Double, r: Double) = {
    assertNotZero(r)
    l / r
  }
  override def sum(t: TraversableOnce[Double]): Double = {
    var sum = 0.0
    t.foreach(sum += _)
    sum
  }
  override def sumOption(t: TraversableOnce[Double]): Option[Double] =
    if (t.isEmpty) None
    else Some(sum(t))
}

object BooleanField extends Field[Boolean] {
  override def one = true
  override def zero = false
  override def negate(v: Boolean) = v
  override def plus(l: Boolean, r: Boolean) = l ^ r
  override def minus(l: Boolean, r: Boolean) = l ^ r
  override def times(l: Boolean, r: Boolean) = l && r
  override def inverse(l: Boolean) = {
    assertNotZero(l)
    true
  }
  override def div(l: Boolean, r: Boolean) = {
    assertNotZero(r)
    l
  }
}

object Field {
  // This pattern is really useful for typeclasses
  def div[T](l: T, r: T)(implicit fld: Field[T]) = fld.div(l, r)

  implicit val boolField: Field[Boolean] = BooleanField
  implicit val jboolField: Field[JBool] = JBoolField
  implicit val floatField: Field[Float] = FloatField
  implicit val jfloatField: Field[JFloat] = JFloatField
  implicit val doubleField: Field[Double] = DoubleField
  implicit val jdoubleField: Field[JDouble] = JDoubleField
}
