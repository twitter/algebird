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

import algebra.{Group => AGroup}
import algebra.ring.AdditiveGroup
import java.lang.{
  Boolean => JBool,
  Double => JDouble,
  Float => JFloat,
  Integer => JInt,
  Long => JLong,
  Short => JShort
}

import scala.reflect.ClassTag

import scala.annotation.implicitNotFound
import scala.math.Equiv

/**
 * Group: this is a monoid that also has subtraction (and negation): So, you can do (a-b), or -a (which is
 * equal to 0 - a).
 */

@implicitNotFound(msg = "Cannot find Group type class for ${T}")
trait Group[@specialized(Int, Long, Float, Double) T] extends AGroup[T] with Monoid[T] with AdditiveGroup[T] {
  /*
   * This are from algebra.Group
   */
  override def additive: AGroup[T] = this
  override def remove(l: T, r: T): T = minus(l, r)
  override def inverse(v: T): T = negate(v)
}

// For Java interop so they get the default methods
abstract class AbstractGroup[T] extends Group[T]

// Trivial group. Returns constant on any interaction.
// The contract is that T be a singleton type (that is, t1 == t2 returns true
// for all instances t1,t2 of type T).
class ConstantGroup[T](constant: T) extends Group[T] {
  override def zero: T = constant
  override def negate(u: T): T = constant
  override def plus(l: T, r: T): T = constant
  override def sumOption(iter: TraversableOnce[T]): Option[T] =
    if (iter.isEmpty) None
    else Some(constant)
}

// Trivial group, but possibly useful to make a group of (Unit, T) for some T.
object UnitGroup extends ConstantGroup[Unit](())

// similar to the above:
object NullGroup extends ConstantGroup[Null](null)

/**
 * Some(5) - Some(3) == Some(2) Some(5) - Some(5) == None negate Some(5) == Some(-5) Note: Some(0) and None
 * are equivalent under this Group
 */
class OptionGroup[T](implicit group: Group[T]) extends OptionMonoid[T] with Group[Option[T]] {

  override def isNonZero(opt: Option[T]): Boolean =
    opt.exists(group.isNonZero(_))

  override def negate(opt: Option[T]): Option[T] =
    opt.map(v => group.negate(v))
}

/**
 * Extends pair-wise sum Array monoid into a Group negate is defined as the negation of each element of the
 * array.
 */
class ArrayGroup[T: ClassTag](implicit grp: Group[T]) extends ArrayMonoid[T]() with Group[Array[T]] {
  override def negate(g: Array[T]): Array[T] = {
    val res = new Array[T](g.length)
    var idx = 0
    while (idx < res.length) {
      res(idx) = grp.negate(g(idx))
      idx = idx + 1
    }

    res
  }
}

class FromAlgebraGroup[T](m: AGroup[T]) extends FromAlgebraMonoid(m) with Group[T] {
  override def negate(t: T): T = m.inverse(t)
  override def minus(r: T, l: T): T = m.remove(r, l)
}

private[algebird] trait FromAlgebraGroupImplicit1 {
  implicit def fromAlgebraAdditiveGroup[T](implicit m: AdditiveGroup[T]): Group[T] =
    new FromAlgebraGroup(m.additive)
}
private[algebird] trait FromAlgebraGroupImplicit0 extends FromAlgebraGroupImplicit1 {
  implicit def fromAlgebraGroup[T](implicit m: AGroup[T]): Group[T] =
    new FromAlgebraGroup(m)
}

object Group extends GeneratedGroupImplicits with ProductGroups with FromAlgebraGroupImplicit0 {
  // This pattern is really useful for typeclasses
  def negate[T](x: T)(implicit grp: Group[T]): T = grp.negate(x)
  def minus[T](l: T, r: T)(implicit grp: Group[T]): T = grp.minus(l, r)
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

  implicit def nullGroup: Group[Null] = NullGroup
  implicit def unitGroup: Group[Unit] = UnitGroup
  implicit def boolGroup: Group[Boolean] = BooleanRing
  implicit def jboolGroup: Group[JBool] = JBoolRing
  implicit def intGroup: Group[Int] = IntRing
  implicit def jintGroup: Group[JInt] = JIntRing
  implicit def shortGroup: Group[Short] = ShortRing
  implicit def jshortGroup: Group[JShort] = JShortRing
  implicit def longGroup: Group[Long] = LongRing
  implicit def bigIntGroup: Group[BigInt] = BigIntRing
  implicit def bigDecimalGroup: Group[BigDecimal] = implicitly[Ring[BigDecimal]]
  implicit def jlongGroup: Group[JLong] = JLongRing
  implicit def floatGroup: Group[Float] = FloatRing
  implicit def jfloatGroup: Group[JFloat] = JFloatRing
  implicit def doubleGroup: Group[Double] = DoubleRing
  implicit def jdoubleGroup: Group[JDouble] = JDoubleRing
  implicit def optionGroup[T: Group]: OptionGroup[T] = new OptionGroup[T]
  implicit def indexedSeqGroup[T: Group]: Group[IndexedSeq[T]] =
    new IndexedSeqGroup[T]
  implicit def mapGroup[K, V](implicit group: Group[V]): Group[Map[K, V]] =
    new MapGroup[K, V]()(group)
  implicit def scMapGroup[K, V](implicit group: Group[V]): Group[scala.collection.Map[K, V]] =
    new ScMapGroup[K, V]()(group)
}
