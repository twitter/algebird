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

import algebra.{ Semigroup => ASemigroup }
import algebra.ring.{ AdditiveSemigroup }
import java.lang.{ Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool }
import java.util.{ List => JList, Map => JMap }

import scala.collection.mutable.{ Map => MMap }
import scala.collection.{ Map => ScMap }
import scala.annotation.{ implicitNotFound, tailrec }

import macros.caseclass._

/**
 * A semigroup is any type `T` with an associative operation (`plus`):
 *
 * {{{
 * a plus (b plus c) = (a plus b) plus c
 * }}}
 *
 * Example instances:
 *  - `Semigroup[Int]`: `plus` `Int#+`
 *  - `Semigroup[List[T]]`: `plus` is `List#++`
 *
 * @define T T
 */
@implicitNotFound(msg = "Cannot find Semigroup type class for ${T}")
trait Semigroup[@specialized(Int, Long, Float, Double) T] extends ASemigroup[T] with AdditiveSemigroup[T] {
  /**
   * Returns an instance of `$T` calculated by summing all instances in
   * `iter` in one pass. Returns `None` if `iter` is empty, else
   * `Some[$T]`.
   *
   * @param iter instances of `$T` to be combined
   * @return `None` if `iter` is empty, else an option value containing the summed `$T`
   * @note Override if there is a faster way to compute this sum than
   *       `iter.reduceLeftOption` using [[plus]].
   */
  def sumOption(iter: TraversableOnce[T]): Option[T] =
    iter.reduceLeftOption { plus(_, _) }

  /*
   * These are methods from algebra
   */
  override def trySum(iter: TraversableOnce[T]): Option[T] = sumOption(iter)

  override def additive: ASemigroup[T] = this
  override def combine(l: T, r: T): T = plus(l, r)
  override def combineAllOption(iter: TraversableOnce[T]): Option[T] = sumOption(iter)
}

// For Java interop so they get the default sumOption
abstract class AbstractSemigroup[T] extends Semigroup[T]

/**
 * Either semigroup is useful for error handling.
 * if everything is correct, use Right (it's right, get it?), if something goes
 * wrong, use Left.  plus does the normal thing for plus(Right, Right), or plus(Left, Left),
 * but if exactly one is Left, we return that value (to keep the error condition).
 * Typically, the left value will be a string representing the errors.
 *
 * @define T Either[L, R]
 */
class EitherSemigroup[L, R](implicit semigroupl: Semigroup[L], semigroupr: Semigroup[R]) extends Semigroup[Either[L, R]] {

  override def plus(l: Either[L, R], r: Either[L, R]) = {
    if (l.isLeft) {
      // l is Left, r may or may not be:
      if (r.isRight) {
        //Avoid the allocation:
        l
      } else {
        //combine the lefts:
        Left(semigroupl.plus(l.left.get, r.left.get))
      }
    } else if (r.isLeft) {
      //l is not a Left value, so just return right:
      r
    } else {
      //both l and r are Right values:
      Right(semigroupr.plus(l.right.get, r.right.get))
    }
  }
}

class FromAlgebraSemigroup[T](sg: ASemigroup[T]) extends Semigroup[T] {
  override def plus(l: T, r: T): T = sg.combine(l, r)
  override def sumOption(ts: TraversableOnce[T]): Option[T] = sg.combineAllOption(ts)
}

/**
 * An Algebra semigroup can be an Algebird semigroup
 */
private[algebird] trait FromAlgebraSemigroupImplicit1 {
  implicit def fromAlgebraAdditiveSemigroup[T](implicit sg: AdditiveSemigroup[T]): Semigroup[T] =
    new FromAlgebraSemigroup(sg.additive)
}

private[algebird] trait FromAlgebraSemigroupImplicit0 extends FromAlgebraSemigroupImplicit1 {
  implicit def fromAlgebraSemigroup[T](implicit sg: ASemigroup[T]): Semigroup[T] =
    new FromAlgebraSemigroup(sg)
}

object Semigroup extends GeneratedSemigroupImplicits with ProductSemigroups with FromAlgebraSemigroupImplicit0 {
  def maybePlus[T](opt: Option[T], t: T)(implicit sg: Semigroup[T]): T =
    opt match {
      case None => t
      case Some(s) => sg.plus(s, t)
    }
  def maybePlus[T](t: T, opt: Option[T])(implicit sg: Semigroup[T]): T =
    opt match {
      case None => t
      case Some(u) => sg.plus(t, u)
    }
  // This pattern is really useful for typeclasses
  def plus[T](l: T, r: T)(implicit semi: Semigroup[T]) = semi.plus(l, r)
  // Left sum: (((a + b) + c) + d)
  def sumOption[T](iter: TraversableOnce[T])(implicit sg: Semigroup[T]): Option[T] =
    sg.sumOption(iter)

  def from[T](associativeFn: (T, T) => T): Semigroup[T] =
    new Semigroup[T] { def plus(l: T, r: T) = associativeFn(l, r) }

  /**
   * Same as v + v + v .. + v (i times in total)
   * requires i > 0, wish we had PositiveBigInt as a class
   */
  def intTimes[T](i: BigInt, v: T)(implicit sg: Semigroup[T]): T = {
    require(i > 0, "Cannot do non-positive products with a Semigroup, try Monoid/Group.intTimes")
    intTimesRec(i - 1, v, 0, (v, Vector[T]()))
  }

  @tailrec
  private def intTimesRec[T](i: BigInt, v: T, pow: Int, vaccMemo: (T, Vector[T]))(implicit sg: Semigroup[T]): T = {
    if (i == 0) {
      vaccMemo._1
    } else {
      /* i2 = i % 2
       * 2^pow(i*v) + acc == 2^(pow+1)((i/2)*v) + (acc + 2^pow i2 * v)
       */
      val half = i / 2
      val rem = i % 2
      val newAccMemo = if (rem == 0) vaccMemo else {
        val (res, newMemo) = timesPow2(pow, v, vaccMemo._2)
        (sg.plus(vaccMemo._1, res), newMemo)
      }
      intTimesRec(half, v, pow + 1, newAccMemo)
    }
  }

  // Returns (2^power) * v = (2^(power - 1) v + 2^(power - 1) v)
  private def timesPow2[T](power: Int, v: T, memo: Vector[T])(implicit sg: Semigroup[T]): (T, Vector[T]) = {
    val size = memo.size
    require(power >= 0, "power cannot be negative")
    if (power == 0) {
      (v, memo)
    } else if (power <= size) {
      (memo(power - 1), memo)
    } else {
      var item = if (size == 0) v else memo.last
      var pow = size
      var newMemo = memo
      while (pow < power) {
        // x = 2*x
        item = sg.plus(item, item)
        pow += 1
        newMemo = newMemo :+ item
      }
      (item, newMemo)
    }
  }

  implicit def nullSemigroup: Semigroup[Null] = NullGroup
  implicit def unitSemigroup: Semigroup[Unit] = UnitGroup
  implicit def boolSemigroup: Semigroup[Boolean] = BooleanRing
  implicit def jboolSemigroup: Semigroup[JBool] = JBoolRing
  implicit def intSemigroup: Semigroup[Int] = IntRing
  implicit def jintSemigroup: Semigroup[JInt] = JIntRing
  implicit def shortSemigroup: Semigroup[Short] = ShortRing
  implicit def jshortSemigroup: Semigroup[JShort] = JShortRing
  implicit def longSemigroup: Semigroup[Long] = LongRing
  implicit def bigIntSemigroup: Semigroup[BigInt] = BigIntRing
  implicit def bigDecimalSemigroup: Semigroup[BigDecimal] = implicitly[Ring[BigDecimal]]
  implicit def jlongSemigroup: Semigroup[JLong] = JLongRing
  implicit def floatSemigroup: Semigroup[Float] = FloatRing
  implicit def jfloatSemigroup: Semigroup[JFloat] = JFloatRing
  implicit def doubleSemigroup: Semigroup[Double] = DoubleRing
  implicit def jdoubleSemigroup: Semigroup[JDouble] = JDoubleRing
  implicit def stringSemigroup: Semigroup[String] = StringMonoid
  implicit def optionSemigroup[T: Semigroup]: Semigroup[Option[T]] = new OptionMonoid[T]
  implicit def listSemigroup[T]: Semigroup[List[T]] = new ListMonoid[T]
  implicit def seqSemigroup[T]: Semigroup[Seq[T]] = new SeqMonoid[T]
  implicit def indexedSeqSemigroup[T: Semigroup]: Semigroup[IndexedSeq[T]] = new IndexedSeqSemigroup[T]
  implicit def jlistSemigroup[T]: Semigroup[JList[T]] = new JListMonoid[T]
  implicit def setSemigroup[T]: Semigroup[Set[T]] = new SetMonoid[T]
  implicit def mapSemigroup[K, V: Semigroup]: Semigroup[Map[K, V]] = new MapMonoid[K, V]
  implicit def scMapSemigroup[K, V: Semigroup]: Semigroup[ScMap[K, V]] = new ScMapMonoid[K, V]
  implicit def jmapSemigroup[K, V: Semigroup]: Semigroup[JMap[K, V]] = new JMapMonoid[K, V]
  implicit def eitherSemigroup[L: Semigroup, R: Semigroup]: Semigroup[Either[L, R]] = new EitherSemigroup[L, R]
  implicit def function1Semigroup[T]: Semigroup[Function1[T, T]] = new Function1Monoid[T]
}
