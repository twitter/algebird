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

import java.lang.{Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool}
import java.util.{List => JList, Map => JMap}

import scala.annotation.implicitNotFound
import scala.math.Equiv
/**
 * Group: this is a monoid that also has subtraction (and negation):
 *   So, you can do (a-b), or -a (which is equal to 0 - a).
 */

@implicitNotFound(msg = "Cannot find Group type class for ${T}")
trait Group[@specialized(Int,Long,Float,Double) T] extends Monoid[T] {
  // must override negate or minus (or both)
  def negate(v : T) : T = minus(zero, v)
  def minus(l : T, r : T) : T = plus(l, negate(r))
}

// Trivial group. Returns constant on any interaction.
// The contract is that T be a singleton type (that is, t1 == t2 returns true
// for all instances t1,t2 of type T).
class ConstantGroup[T](constant: T) extends Group[T] {
  override def zero = constant
  override def negate(u : T) = constant
  override def plus(l : T, r : T) = constant
}

// Trivial group, but possibly useful to make a group of (Unit, T) for some T.
object UnitGroup extends ConstantGroup[Unit](())

// similar to the above:
object NullGroup extends ConstantGroup[Null](null)

object Group extends GeneratedGroupImplicits with ProductGroups {
  // This pattern is really useful for typeclasses
  def negate[T](x : T)(implicit grp : Group[T]) = grp.negate(x)
  def minus[T](l : T, r : T)(implicit grp : Group[T]) = grp.minus(l,r)
  // nonZero and subtraction give an equiv, useful for Map[K,V]
  def equiv[T](implicit grp: Group[T]): Equiv[T] = Equiv.fromFunction[T] { (a, b) =>
    !grp.isNonZero(grp.minus(a, b))
  }
  /** Same as v + v + v .. + v (i times in total) */
  def intTimes[T](i: BigInt, v: T)(implicit grp: Group[T]): T =
    if(i < 0) {
      Monoid.intTimes(-i, grp.negate(v))
    }
    else {
      Monoid.intTimes(i, v)(grp)
    }


  implicit val nullGroup : Group[Null] = NullGroup
  implicit val unitGroup : Group[Unit] = UnitGroup
  implicit val boolGroup : Group[Boolean] = BooleanField
  implicit val jboolGroup : Group[JBool] = JBoolField
  implicit val intGroup : Group[Int] = IntRing
  implicit val jintGroup : Group[JInt] = JIntRing
  implicit val shortGroup : Group[Short] = ShortRing
  implicit val jshortGroup : Group[JShort] = JShortRing
  implicit val longGroup : Group[Long] = LongRing
  implicit val bigIntGroup : Group[BigInt] = BigIntRing
  implicit val jlongGroup : Group[JLong] = JLongRing
  implicit val floatGroup : Group[Float] = FloatField
  implicit val jfloatGroup : Group[JFloat] = JFloatField
  implicit val doubleGroup : Group[Double] = DoubleField
  implicit val jdoubleGroup : Group[JDouble] = JDoubleField
  implicit def indexedSeqGroup[T:Group]: Group[IndexedSeq[T]] = new IndexedSeqGroup[T]
  implicit def mapGroup[K,V](implicit group : Group[V]) = new MapGroup[K,V]()(group)
  implicit def scMapGroup[K,V](implicit group : Group[V]) = new ScMapGroup[K,V]()(group)
}
