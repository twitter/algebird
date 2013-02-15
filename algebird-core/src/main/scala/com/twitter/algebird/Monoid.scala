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

import java.lang.{Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool}
import java.util.{List => JList, Map => JMap}

/**
 * Monoid (take a deep breath, and relax about the weird name):
 *   This is a semigroup that has an additive identity (called zero), such that a+0=a, 0+a=a, for every a
 */

@implicitNotFound(msg = "Cannot find Monoid type class for ${T}")
trait Monoid[@specialized(Int,Long,Float,Double) T] extends Semigroup[T] {
  def zero : T //additive identity
  def assertNotZero(v : T) {
    if(!isNonZero(v)) {
      throw new java.lang.IllegalArgumentException("argument should not be zero")
    }
  }

  override def isNonZero(v : T) = (v != zero)

  def nonZeroOption(v : T): Option[T] = {
    if (isNonZero(v)) {
      Some(v)
    }
    else {
      None
    }
  }
  @deprecated("Just use Monoid.sum")
  def sum(vs: TraversableOnce[T]): T = Monoid.sum(vs)(this)
}

/**
 * Some(5) + Some(3) == Some(8)
 * Some(5) + None == Some(5)
 */
class OptionMonoid[T](implicit semi : Semigroup[T]) extends Monoid[Option[T]] {
  def zero = None
  def plus(left : Option[T], right : Option[T]) : Option[T] = {
    if(left.isEmpty) {
      right
    }
    else if(right.isEmpty) {
      left
    }
    else {
      Some(semi.plus(left.get, right.get))
    }
  }
}

class EitherMonoid[L,R](implicit semigroupl : Semigroup[L], monoidr : Monoid[R]) extends EitherSemigroup[L, R]()(semigroupl, monoidr) with Monoid[Either[L,R]] {
  override lazy val zero = Right(monoidr.zero)
}

object StringMonoid extends Monoid[String] {
  override val zero = ""
  override def plus(left : String, right : String) = left + right
}

/** List concatenation monoid.
 * plus means concatenation, zero is empty list
 */
class ListMonoid[T] extends Monoid[List[T]] {
  override def zero = List[T]()
  override def plus(left : List[T], right : List[T]) = left ++ right
}

// equivalent to ListMonoid
class SeqMonoid[T] extends Monoid[Seq[T]] {
  override def zero = Seq[T]()
  override def plus(left : Seq[T], right : Seq[T]) = left ++ right
}

/** Set union monoid.
 * plus means union, zero is empty set
 */
class SetMonoid[T] extends Monoid[Set[T]] {
  override def zero = Set[T]()
  override def plus(left : Set[T], right : Set[T]) = left ++ right
}

/** Function1 monoid.
 * plus means function composition, zero is the identity function
 */
class Function1Monoid[T] extends Monoid[Function1[T,T]] {
  override def zero = {
    (t : T) => t
  }

  // (f1 + f2)(x) = f2(f1(x)) so that:
  // listOfFn.foldLeft(x) { (v, fn) => fn(v) } = (Monoid.sum(listOfFn))(x)
  override def plus(f1 : Function1[T,T], f2 : Function1[T,T]) = {
    (t : T) => f2(f1(t))
  }
}

object Monoid extends GeneratedMonoidImplicits {
  // This pattern is really useful for typeclasses
  def zero[T](implicit mon : Monoid[T]) = mon.zero
  // strictly speaking, same as Semigroup, but most interesting examples
  // are monoids, and code already depends on this:
  def plus[T](l: T, r: T)(implicit monoid: Monoid[T]): T = monoid.plus(l,r)
  def assertNotZero[T](v: T)(implicit monoid: Monoid[T]) = monoid.assertNotZero(v)
  def isNonZero[T](v: T)(implicit monoid: Monoid[T]) = monoid.isNonZero(v)
  def nonZeroOption[T](v: T)(implicit monoid: Monoid[T]) = monoid.nonZeroOption(v)
  // Left sum: (((a + b) + c) + d)
  def sum[T](iter : TraversableOnce[T])(implicit monoid: Monoid[T]): T = {
    Semigroup.sumOption(iter)(monoid).getOrElse(monoid.zero)
  }

  def from[T](z: => T)(associativeFn: (T,T) => T): Monoid[T] = new Monoid[T] {
    lazy val zero = z
    def plus(l:T, r:T) = associativeFn(l,r)
  }

  implicit val nullMonoid : Monoid[Null] = NullGroup
  implicit val unitMonoid : Monoid[Unit] = UnitGroup
  implicit val boolMonoid : Monoid[Boolean] = BooleanField
  implicit val jboolMonoid : Monoid[JBool] = JBoolField
  implicit val intMonoid : Monoid[Int] = IntRing
  implicit val jintMonoid : Monoid[JInt] = JIntRing
  implicit val shortMonoid : Monoid[Short] = ShortRing
  implicit val jshortMonoid : Monoid[JShort] = JShortRing
  implicit val longMonoid : Monoid[Long] = LongRing
  implicit val jlongMonoid : Monoid[JLong] = JLongRing
  implicit val floatMonoid : Monoid[Float] = FloatField
  implicit val jfloatMonoid : Monoid[JFloat] = JFloatField
  implicit val doubleMonoid : Monoid[Double] = DoubleField
  implicit val jdoubleMonoid : Monoid[JDouble] = JDoubleField
  implicit val stringMonoid : Monoid[String] = StringMonoid
  implicit def optionMonoid[T : Semigroup] = new OptionMonoid[T]
  implicit def listMonoid[T] : Monoid[List[T]] = new ListMonoid[T]
  implicit def seqMonoid[T] : Monoid[Seq[T]] = new SeqMonoid[T]
  implicit def indexedSeqMonoid[T:Monoid]: Monoid[IndexedSeq[T]] = new IndexedSeqMonoid[T]
  implicit def jlistMonoid[T] : Monoid[JList[T]] = new JListMonoid[T]
  implicit def setMonoid[T] : Monoid[Set[T]] = new SetMonoid[T]
  implicit def mapMonoid[K,V: Semigroup] = new MapMonoid[K,V]
  implicit def jmapMonoid[K,V : Semigroup] = new JMapMonoid[K,V]
  implicit def eitherMonoid[L : Semigroup, R : Monoid] = new EitherMonoid[L, R]
  implicit def function1Monoid[T] = new Function1Monoid[T]
}
