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

import algebra.{ Group => AGroup }
import java.lang.{ Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool }
import java.util.{ List => JList, Map => JMap }

import scala.reflect.ClassTag

import scala.annotation.implicitNotFound
import scala.math.Equiv
/**
 * Group: this is a monoid that also has subtraction (and negation):
 *   So, you can do (a-b), or -a (which is equal to 0 - a).
 */

@implicitNotFound(msg = "Cannot find Group type class for ${T}")
trait Group[@specialized(Int, Long, Float, Double) T] extends Monoid[T] {
  // must override negate or minus (or both)
  def negate(v: T): T = minus(zero, v)
  def minus(l: T, r: T): T = plus(l, negate(r))
}

// For Java interop so they get the default methods
abstract class AbstractGroup[T] extends Group[T]

// Trivial group. Returns constant on any interaction.
// The contract is that T be a singleton type (that is, t1 == t2 returns true
// for all instances t1,t2 of type T).
class ConstantGroup[T](constant: T) extends Group[T] {
  override def zero = constant
  override def negate(u: T) = constant
  override def plus(l: T, r: T) = constant
  override def sumOption(iter: TraversableOnce[T]): Option[T] =
    if (iter.isEmpty) None
    else Some(constant)
}

// Trivial group, but possibly useful to make a group of (Unit, T) for some T.
object UnitGroup extends ConstantGroup[Unit](())

// similar to the above:
object NullGroup extends ConstantGroup[Null](null)

/**
 * Some(5) - Some(3) == Some(2)
 * Some(5) - Some(5) == None
 * negate Some(5) == Some(-5)
 * Note: Some(0) and None are equivalent under this Group
 */
class OptionGroup[T](implicit group: Group[T]) extends OptionMonoid[T]
  with Group[Option[T]] {

  override def isNonZero(opt: Option[T]): Boolean =
    opt.exists{ group.isNonZero(_) }

  override def negate(opt: Option[T]) =
    opt.map{ v => group.negate(v) }
}

/**
 * Extends pair-wise sum Array monoid into a Group
 * negate is defined as the negation of each element of the array.
 */
class ArrayGroup[T: ClassTag](implicit grp: Group[T])
  extends ArrayMonoid[T]() with Group[Array[T]] {
  override def negate(g: Array[T]): Array[T] = g.map {
    grp.negate(_)
  }.toArray
}

/**
 * Group can't extend AGroup because Field extends Group and it already has
 * a method named inverse
 */
class FromAlgebraGroup[T](m: AGroup[T]) extends FromAlgebraMonoid(m) with Group[T] {
  override def negate(t: T): T = m.inverse(t)
  override def minus(r: T, l: T): T = m.remove(r, l)
}

class ToAlgebraGroup[T](g: Group[T]) extends AGroup[T] {
  override def empty: T = g.zero
  override def combine(l: T, r: T): T = g.plus(l, r)
  override def combineAll(ts: TraversableOnce[T]): T = g.sum(ts)
  override def combineAllOption(ts: TraversableOnce[T]): Option[T] = g.sumOption(ts)
  override def remove(l: T, r: T): T = g.minus(l, r)
  override def inverse(v: T): T = g.negate(v)
}

trait FromAlgebraGroupImplicit {
  implicit def fromAlgebraGroup[T](m: AGroup[T]): Group[T] = new FromAlgebraGroup(m)
}

object Group extends GeneratedGroupImplicits with ProductGroups with FromAlgebraGroupImplicit {
  // This pattern is really useful for typeclasses
  def negate[T](x: T)(implicit grp: Group[T]) = grp.negate(x)
  def minus[T](l: T, r: T)(implicit grp: Group[T]) = grp.minus(l, r)
  // nonZero and subtraction give an equiv, useful for Map[K,V]
  def equiv[T](implicit grp: Group[T]): Equiv[T] = Equiv.fromFunction[T] { (a, b) =>
    !grp.isNonZero(grp.minus(a, b))
  }
  /** Same as v + v + v .. + v (i times in total) */
  def intTimes[T](i: BigInt, v: T)(implicit grp: Group[T]): T =
    if (i < 0) {
      Monoid.intTimes(-i, grp.negate(v))
    } else {
      Monoid.intTimes(i, v)(grp)
    }

  implicit val nullGroup: Group[Null] = NullGroup
  implicit val unitGroup: Group[Unit] = UnitGroup
  implicit val boolGroup: Group[Boolean] = BooleanField
  implicit val jboolGroup: Group[JBool] = JBoolField
  implicit val intGroup: Group[Int] = IntRing
  implicit val jintGroup: Group[JInt] = JIntRing
  implicit val shortGroup: Group[Short] = ShortRing
  implicit val jshortGroup: Group[JShort] = JShortRing
  implicit val longGroup: Group[Long] = LongRing
  implicit val bigIntGroup: Group[BigInt] = BigIntRing
  implicit val jlongGroup: Group[JLong] = JLongRing
  implicit val floatGroup: Group[Float] = FloatField
  implicit val jfloatGroup: Group[JFloat] = JFloatField
  implicit val doubleGroup: Group[Double] = DoubleField
  implicit val jdoubleGroup: Group[JDouble] = JDoubleField
  implicit def optionGroup[T: Group] = new OptionGroup[T]
  implicit def indexedSeqGroup[T: Group]: Group[IndexedSeq[T]] = new IndexedSeqGroup[T]
  implicit def mapGroup[K, V](implicit group: Group[V]) = new MapGroup[K, V]()(group)
  implicit def scMapGroup[K, V](implicit group: Group[V]) = new ScMapGroup[K, V]()(group)
}
