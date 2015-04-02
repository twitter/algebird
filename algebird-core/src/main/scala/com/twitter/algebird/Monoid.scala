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

import scala.annotation.implicitNotFound
import scala.math.Equiv
import scala.reflect.ClassTag

import java.lang.{ Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool }
import java.util.{ List => JList, Map => JMap }

import scala.collection.mutable.{ Map => MMap }
import scala.collection.{ Map => ScMap }

/**
 * HasAdditionOperatorAndZero (take a deep breath, and relax about the weird name):
 *   This is a semigroup that has an additive identity (called zero), such that a+0=a, 0+a=a, for every a
 */

@implicitNotFound(msg = "Cannot find HasAdditionOperatorAndZero type class for ${T}")
trait HasAdditionOperatorAndZero[@specialized(Int, Long, Float, Double) T] extends HasAdditionOperator[T] {
  def zero: T //additive identity
  def isNonZero(v: T): Boolean = (v != zero)
  def assertNotZero(v: T) {
    if (!isNonZero(v)) {
      throw new java.lang.IllegalArgumentException("argument should not be zero")
    }
  }

  def nonZeroOption(v: T): Option[T] = {
    if (isNonZero(v)) {
      Some(v)
    } else {
      None
    }
  }
  // Override this if there is a more efficient means to implement this
  def sum(vs: TraversableOnce[T]): T = sumOption(vs).getOrElse(zero)
}

// For Java interop so they get the default methods
abstract class AbstractHasAdditionOperatorAndZero[T] extends HasAdditionOperatorAndZero[T]

/**
 * Some(5) + Some(3) == Some(8)
 * Some(5) + None == Some(5)
 */
class OptionHasAdditionOperatorAndZero[T](implicit semi: HasAdditionOperator[T]) extends HasAdditionOperatorAndZero[Option[T]] {
  def zero = None
  def plus(left: Option[T], right: Option[T]): Option[T] = {
    if (left.isEmpty) {
      right
    } else if (right.isEmpty) {
      left
    } else {
      Some(semi.plus(left.get, right.get))
    }
  }
  override def sumOption(items: TraversableOnce[Option[T]]): Option[Option[T]] =
    if (items.isEmpty) None
    else Some(HasAdditionOperator.sumOption(items.filter(_.isDefined).map { _.get }))
}

class EitherHasAdditionOperatorAndZero[L, R](implicit semigroupl: HasAdditionOperator[L], monoidr: HasAdditionOperatorAndZero[R]) extends EitherHasAdditionOperator[L, R]()(semigroupl, monoidr) with HasAdditionOperatorAndZero[Either[L, R]] {
  override lazy val zero = Right(monoidr.zero)
}

object StringHasAdditionOperatorAndZero extends HasAdditionOperatorAndZero[String] {
  override val zero = ""
  override def plus(left: String, right: String) = left + right
  override def sumOption(items: TraversableOnce[String]): Option[String] =
    if (items.isEmpty) None
    else Some(items.mkString(""))
}

/**
 * List concatenation monoid.
 * plus means concatenation, zero is empty list
 */
class ListHasAdditionOperatorAndZero[T] extends HasAdditionOperatorAndZero[List[T]] {
  override def zero = List[T]()
  override def plus(left: List[T], right: List[T]) = left ++ right
  override def sumOption(items: TraversableOnce[List[T]]): Option[List[T]] =
    if (items.isEmpty) None
    else {
      // ListBuilder mutates the tail of the list until
      // result is called so that it is O(N) to push N things on, not N^2
      val builder = List.newBuilder[T]
      items.foreach { builder ++= _ }
      Some(builder.result())
    }
}

// equivalent to ListHasAdditionOperatorAndZero
class SeqHasAdditionOperatorAndZero[T] extends HasAdditionOperatorAndZero[Seq[T]] {
  override def zero = Seq[T]()
  override def plus(left: Seq[T], right: Seq[T]) = left ++ right
  override def sumOption(items: TraversableOnce[Seq[T]]): Option[Seq[T]] =
    if (items.isEmpty) None
    else {
      val builder = Seq.newBuilder[T]
      items.foreach { builder ++= _ }
      Some(builder.result())
    }
}

/**
 * Pair-wise sum Array monoid.
 *
 * plus returns left[i] + right[i] for all array elements.
 * The resulting array will be as long as the longest array (with its elements duplicated)
 * zero is an empty array
 */
class ArrayHasAdditionOperatorAndZero[T: ClassTag](implicit semi: HasAdditionOperator[T]) extends HasAdditionOperatorAndZero[Array[T]] {

  //additive identity
  override def isNonZero(v: Array[T]): Boolean = v.nonEmpty

  override def zero = Array[T]()
  override def plus(left: Array[T], right: Array[T]) = {
    val (longer, shorter) = if (left.length > right.length) (left, right) else (right, left)
    val sum = longer.clone
    for (i <- 0 until shorter.length)
      sum.update(i, semi.plus(sum(i), shorter(i)))

    sum
  }
}

/**
 * Set union monoid.
 * plus means union, zero is empty set
 */
class SetHasAdditionOperatorAndZero[T] extends HasAdditionOperatorAndZero[Set[T]] {
  override def zero = Set[T]()
  override def plus(left: Set[T], right: Set[T]) = left ++ right
  override def sumOption(items: TraversableOnce[Set[T]]): Option[Set[T]] =
    if (items.isEmpty) None
    else {
      val mutable = scala.collection.mutable.Set[T]()
      items.foreach { s => mutable ++= s }
      Some(mutable.toSet)
    }
}

/**
 * Function1 monoid.
 * plus means function composition, zero is the identity function
 */
class Function1HasAdditionOperatorAndZero[T] extends HasAdditionOperatorAndZero[Function1[T, T]] {
  override def zero = identity[T]

  // (f1 + f2)(x) = f2(f1(x)) so that:
  // listOfFn.foldLeft(x) { (v, fn) => fn(v) } = (HasAdditionOperatorAndZero.sum(listOfFn))(x)
  override def plus(f1: Function1[T, T], f2: Function1[T, T]) = {
    (t: T) => f2(f1(t))
  }
}

// To use the OrValHasAdditionOperatorAndZero wrap your item in a OrVal object
case class OrVal(get: Boolean) extends AnyVal

object OrVal {
  implicit def monoid: HasAdditionOperatorAndZero[OrVal] = OrValHasAdditionOperatorAndZero
  def unboxedHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Boolean] = new HasAdditionOperatorAndZero[Boolean] {
    def zero = false
    def plus(l: Boolean, r: Boolean) = l || r
    override def sumOption(its: TraversableOnce[Boolean]) =
      if (its.isEmpty) None
      else Some(its.exists(identity))
  }
}

/**
 * Boolean OR monoid.
 * plus means logical OR, zero is false.
 */
object OrValHasAdditionOperatorAndZero extends HasAdditionOperatorAndZero[OrVal] {
  override def zero = OrVal(false)
  override def plus(l: OrVal, r: OrVal) = if (l.get) l else r
  override def sumOption(its: TraversableOnce[OrVal]) =
    if (its.isEmpty) None
    else Some(OrVal(its.exists(_.get)))
}

// To use the AndValHasAdditionOperatorAndZero wrap your item in a AndVal object
case class AndVal(get: Boolean) extends AnyVal

object AndVal {
  implicit def monoid: HasAdditionOperatorAndZero[AndVal] = AndValHasAdditionOperatorAndZero
  def unboxedHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Boolean] = new HasAdditionOperatorAndZero[Boolean] {
    def zero = true
    def plus(l: Boolean, r: Boolean) = l && r
    override def sumOption(its: TraversableOnce[Boolean]) =
      if (its.isEmpty) None
      else Some(its.forall(identity))
  }
}

/**
 * Boolean AND monoid.
 * plus means logical AND, zero is true.
 */
object AndValHasAdditionOperatorAndZero extends HasAdditionOperatorAndZero[AndVal] {
  override def zero = AndVal(true)
  override def plus(l: AndVal, r: AndVal) = if (l.get) r else l
  override def sumOption(its: TraversableOnce[AndVal]) =
    if (its.isEmpty) None
    else Some(AndVal(its.forall(_.get)))
}

object HasAdditionOperatorAndZero extends GeneratedHasAdditionOperatorAndZeroImplicits with ProductHasAdditionOperatorAndZeros {
  // This pattern is really useful for typeclasses
  def zero[T](implicit mon: HasAdditionOperatorAndZero[T]) = mon.zero
  // strictly speaking, same as HasAdditionOperator, but most interesting examples
  // are monoids, and code already depends on this:
  def plus[T](l: T, r: T)(implicit monoid: HasAdditionOperatorAndZero[T]): T = monoid.plus(l, r)
  def assertNotZero[T](v: T)(implicit monoid: HasAdditionOperatorAndZero[T]) = monoid.assertNotZero(v)
  def isNonZero[T](v: T)(implicit monoid: HasAdditionOperatorAndZero[T]) = monoid.isNonZero(v)
  def nonZeroOption[T](v: T)(implicit monoid: HasAdditionOperatorAndZero[T]) = monoid.nonZeroOption(v)
  // Left sum: (((a + b) + c) + d)
  def sum[T](iter: TraversableOnce[T])(implicit monoid: HasAdditionOperatorAndZero[T]): T =
    monoid.sum(iter)

  def from[T](z: => T)(associativeFn: (T, T) => T): HasAdditionOperatorAndZero[T] = new HasAdditionOperatorAndZero[T] {
    lazy val zero = z
    def plus(l: T, r: T) = associativeFn(l, r)
  }

  /**
   * Return an Equiv[T] that uses isNonZero to return equality for all zeros
   * useful for Maps/Vectors that have many equivalent in memory representations of zero
   */
  def zeroEquiv[T: Equiv: HasAdditionOperatorAndZero]: Equiv[T] = Equiv.fromFunction { (a: T, b: T) =>
    (!isNonZero(a) && !isNonZero(b)) || Equiv[T].equiv(a, b)
  }

  /**
   * Same as v + v + v .. + v (i times in total)
   * requires i >= 0, wish we had NonnegativeBigInt as a class
   */
  def intTimes[T](i: BigInt, v: T)(implicit mon: HasAdditionOperatorAndZero[T]): T = {
    require(i >= 0, "Cannot do negative products with a HasAdditionOperatorAndZero, try Group.intTimes")
    if (i == 0) {
      mon.zero
    } else {
      HasAdditionOperator.intTimes(i, v)(mon)
    }
  }

  implicit val nullHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Null] = NullGroup
  implicit val unitHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Unit] = UnitGroup
  implicit val boolHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Boolean] = BooleanField
  implicit val jboolHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[JBool] = JBoolField
  implicit val intHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Int] = IntRing
  implicit val jintHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[JInt] = JIntRing
  implicit val shortHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Short] = ShortRing
  implicit val jshortHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[JShort] = JShortRing
  implicit val bigIntHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[BigInt] = BigIntRing
  implicit val longHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Long] = LongRing
  implicit val jlongHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[JLong] = JLongRing
  implicit val floatHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Float] = FloatField
  implicit val jfloatHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[JFloat] = JFloatField
  implicit val doubleHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[Double] = DoubleField
  implicit val jdoubleHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[JDouble] = JDoubleField
  implicit val stringHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[String] = StringHasAdditionOperatorAndZero
  implicit def optionHasAdditionOperatorAndZero[T: HasAdditionOperator]: HasAdditionOperatorAndZero[Option[T]] = new OptionHasAdditionOperatorAndZero[T]
  implicit def listHasAdditionOperatorAndZero[T]: HasAdditionOperatorAndZero[List[T]] = new ListHasAdditionOperatorAndZero[T]
  implicit def seqHasAdditionOperatorAndZero[T]: HasAdditionOperatorAndZero[Seq[T]] = new SeqHasAdditionOperatorAndZero[T]
  implicit def arrayHasAdditionOperatorAndZero[T: ClassTag](implicit semi: HasAdditionOperator[T]) = new ArrayHasAdditionOperatorAndZero[T]
  implicit def indexedSeqHasAdditionOperatorAndZero[T: HasAdditionOperatorAndZero]: HasAdditionOperatorAndZero[IndexedSeq[T]] = new IndexedSeqHasAdditionOperatorAndZero[T]
  implicit def jlistHasAdditionOperatorAndZero[T]: HasAdditionOperatorAndZero[JList[T]] = new JListHasAdditionOperatorAndZero[T]
  implicit def setHasAdditionOperatorAndZero[T]: HasAdditionOperatorAndZero[Set[T]] = new SetHasAdditionOperatorAndZero[T]
  implicit def mapHasAdditionOperatorAndZero[K, V: HasAdditionOperator]: HasAdditionOperatorAndZero[Map[K, V]] = new MapHasAdditionOperatorAndZero[K, V]
  implicit def scMapHasAdditionOperatorAndZero[K, V: HasAdditionOperator]: HasAdditionOperatorAndZero[ScMap[K, V]] = new ScMapHasAdditionOperatorAndZero[K, V]
  implicit def jmapHasAdditionOperatorAndZero[K, V: HasAdditionOperator]: HasAdditionOperatorAndZero[JMap[K, V]] = new JMapHasAdditionOperatorAndZero[K, V]
  implicit def eitherHasAdditionOperatorAndZero[L: HasAdditionOperator, R: HasAdditionOperatorAndZero]: HasAdditionOperatorAndZero[Either[L, R]] = new EitherHasAdditionOperatorAndZero[L, R]
  implicit def function1HasAdditionOperatorAndZero[T]: HasAdditionOperatorAndZero[Function1[T, T]] = new Function1HasAdditionOperatorAndZero[T]
}
