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
 * Monoid (take a deep breath, and relax about the weird name):
 *   This is a semigroup that has an additive identity (called zero), such that a+0=a, 0+a=a, for every a
 */

@implicitNotFound(msg = "Cannot find Monoid type class for ${T}")
trait Monoid[@specialized(Int, Long, Float, Double) T] extends Semigroup[T] {
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
abstract class AbstractMonoid[T] extends Monoid[T]

/**
 * Some(5) + Some(3) == Some(8)
 * Some(5) + None == Some(5)
 */
class OptionMonoid[T](implicit semi: Semigroup[T]) extends Monoid[Option[T]] {
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
    else Some(semi.sumOption(items.filter(_.isDefined).map { _.get }))
}

class EitherMonoid[L, R](implicit semigroupl: Semigroup[L], monoidr: Monoid[R]) extends EitherSemigroup[L, R]()(semigroupl, monoidr) with Monoid[Either[L, R]] {
  override lazy val zero = Right(monoidr.zero)
}

object StringMonoid extends Monoid[String] {
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
class ListMonoid[T] extends Monoid[List[T]] {
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

// equivalent to ListMonoid
class SeqMonoid[T] extends Monoid[Seq[T]] {
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
class ArrayMonoid[T: ClassTag](implicit semi: Semigroup[T]) extends Monoid[Array[T]] {

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
class SetMonoid[T] extends Monoid[Set[T]] {
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
class Function1Monoid[T] extends Monoid[Function1[T, T]] {
  override def zero = identity[T]

  // (f1 + f2)(x) = f2(f1(x)) so that:
  // listOfFn.foldLeft(x) { (v, fn) => fn(v) } = (Monoid.sum(listOfFn))(x)
  override def plus(f1: Function1[T, T], f2: Function1[T, T]) = {
    (t: T) => f2(f1(t))
  }
}

// To use the OrValMonoid wrap your item in a OrVal object
case class OrVal(get: Boolean) extends AnyVal

object OrVal {
  implicit def monoid: Monoid[OrVal] = OrValMonoid
  def unboxedMonoid: Monoid[Boolean] = new Monoid[Boolean] {
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
object OrValMonoid extends Monoid[OrVal] {
  override def zero = OrVal(false)
  override def plus(l: OrVal, r: OrVal) = if (l.get) l else r
  override def sumOption(its: TraversableOnce[OrVal]) =
    if (its.isEmpty) None
    else Some(OrVal(its.exists(_.get)))
}

// To use the AndValMonoid wrap your item in a AndVal object
case class AndVal(get: Boolean) extends AnyVal

object AndVal {
  implicit def monoid: Monoid[AndVal] = AndValMonoid
  def unboxedMonoid: Monoid[Boolean] = new Monoid[Boolean] {
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
object AndValMonoid extends Monoid[AndVal] {
  override def zero = AndVal(true)
  override def plus(l: AndVal, r: AndVal) = if (l.get) r else l
  override def sumOption(its: TraversableOnce[AndVal]) =
    if (its.isEmpty) None
    else Some(AndVal(its.forall(_.get)))
}

object Monoid extends GeneratedMonoidImplicits with ProductMonoids {
  // This pattern is really useful for typeclasses
  def zero[T](implicit mon: Monoid[T]) = mon.zero
  // strictly speaking, same as Semigroup, but most interesting examples
  // are monoids, and code already depends on this:
  def plus[T](l: T, r: T)(implicit monoid: Monoid[T]): T = monoid.plus(l, r)
  def assertNotZero[T](v: T)(implicit monoid: Monoid[T]) = monoid.assertNotZero(v)
  def isNonZero[T](v: T)(implicit monoid: Monoid[T]) = monoid.isNonZero(v)
  def nonZeroOption[T](v: T)(implicit monoid: Monoid[T]) = monoid.nonZeroOption(v)
  // Left sum: (((a + b) + c) + d)
  def sum[T](iter: TraversableOnce[T])(implicit monoid: Monoid[T]): T =
    monoid.sum(iter)

  def from[T](z: => T)(associativeFn: (T, T) => T): Monoid[T] = new Monoid[T] {
    lazy val zero = z
    def plus(l: T, r: T) = associativeFn(l, r)
  }

  /**
   * Return an Equiv[T] that uses isNonZero to return equality for all zeros
   * useful for Maps/Vectors that have many equivalent in memory representations of zero
   */
  def zeroEquiv[T: Equiv: Monoid]: Equiv[T] = Equiv.fromFunction { (a: T, b: T) =>
    (!isNonZero(a) && !isNonZero(b)) || Equiv[T].equiv(a, b)
  }

  /**
   * Same as v + v + v .. + v (i times in total)
   * requires i >= 0, wish we had NonnegativeBigInt as a class
   */
  def intTimes[T](i: BigInt, v: T)(implicit mon: Monoid[T]): T = {
    require(i >= 0, "Cannot do negative products with a Monoid, try Group.intTimes")
    if (i == 0) {
      mon.zero
    } else {
      Semigroup.intTimes(i, v)(mon)
    }
  }

  implicit val nullMonoid: Monoid[Null] = NullGroup
  implicit val unitMonoid: Monoid[Unit] = UnitGroup
  implicit val boolMonoid: Monoid[Boolean] = BooleanField
  implicit val jboolMonoid: Monoid[JBool] = JBoolField
  implicit val intMonoid: Monoid[Int] = IntRing
  implicit val jintMonoid: Monoid[JInt] = JIntRing
  implicit val shortMonoid: Monoid[Short] = ShortRing
  implicit val jshortMonoid: Monoid[JShort] = JShortRing
  implicit val bigIntMonoid: Monoid[BigInt] = BigIntRing
  implicit val bigDecimalMonoid: Monoid[BigDecimal] = BigDecimalRing
  implicit val longMonoid: Monoid[Long] = LongRing
  implicit val jlongMonoid: Monoid[JLong] = JLongRing
  implicit val floatMonoid: Monoid[Float] = FloatField
  implicit val jfloatMonoid: Monoid[JFloat] = JFloatField
  implicit val doubleMonoid: Monoid[Double] = DoubleField
  implicit val jdoubleMonoid: Monoid[JDouble] = JDoubleField
  implicit val stringMonoid: Monoid[String] = StringMonoid
  implicit def optionMonoid[T: Semigroup]: Monoid[Option[T]] = new OptionMonoid[T]
  implicit def listMonoid[T]: Monoid[List[T]] = new ListMonoid[T]
  implicit def seqMonoid[T]: Monoid[Seq[T]] = new SeqMonoid[T]
  implicit def arrayMonoid[T: ClassTag](implicit semi: Semigroup[T]) = new ArrayMonoid[T]
  implicit def indexedSeqMonoid[T: Monoid]: Monoid[IndexedSeq[T]] = new IndexedSeqMonoid[T]
  implicit def jlistMonoid[T]: Monoid[JList[T]] = new JListMonoid[T]
  implicit def setMonoid[T]: Monoid[Set[T]] = new SetMonoid[T]
  implicit def mapMonoid[K, V: Semigroup]: Monoid[Map[K, V]] = new MapMonoid[K, V]
  implicit def scMapMonoid[K, V: Semigroup]: Monoid[ScMap[K, V]] = new ScMapMonoid[K, V]
  implicit def jmapMonoid[K, V: Semigroup]: Monoid[JMap[K, V]] = new JMapMonoid[K, V]
  implicit def eitherMonoid[L: Semigroup, R: Monoid]: Monoid[Either[L, R]] = new EitherMonoid[L, R]
  implicit def function1Monoid[T]: Monoid[Function1[T, T]] = new Function1Monoid[T]
}
