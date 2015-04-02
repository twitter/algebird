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

import java.lang.{ Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool }
import java.util.{ List => JList, Map => JMap }

import scala.collection.mutable.{ Map => MMap }
import scala.collection.{ Map => ScMap }
import scala.annotation.{ implicitNotFound, tailrec }

/**
 * HasAdditionOperator:
 *   This is a class with a plus method that is associative: a+(b+c) = (a+b)+c
 */
@implicitNotFound(msg = "Cannot find HasAdditionOperator type class for ${T}")
trait HasAdditionOperator[@specialized(Int, Long, Float, Double) T] extends java.io.Serializable {
  def plus(l: T, r: T): T
  /**
   * override this if there is a faster way to do this sum than reduceLeftOption on plus
   */
  def sumOption(iter: TraversableOnce[T]): Option[T] =
    iter.reduceLeftOption { plus(_, _) }
}

// For Java interop so they get the default sumOption
abstract class AbstractHasAdditionOperator[T] extends HasAdditionOperator[T]

/**
 * Either semigroup is useful for error handling.
 * if everything is correct, use Right (it's right, get it?), if something goes
 * wrong, use Left.  plus does the normal thing for plus(Right, Right), or plus(Left, Left),
 * but if exactly one is Left, we return that value (to keep the error condition).
 * Typically, the left value will be a string representing the errors.
 */
class EitherHasAdditionOperator[L, R](implicit semigroupl: HasAdditionOperator[L], semigroupr: HasAdditionOperator[R]) extends HasAdditionOperator[Either[L, R]] {

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

object HasAdditionOperator extends GeneratedHasAdditionOperatorImplicits with ProductHasAdditionOperators {
  // This pattern is really useful for typeclasses
  def plus[T](l: T, r: T)(implicit semi: HasAdditionOperator[T]) = semi.plus(l, r)
  // Left sum: (((a + b) + c) + d)
  def sumOption[T](iter: TraversableOnce[T])(implicit sg: HasAdditionOperator[T]): Option[T] =
    sg.sumOption(iter)

  def from[T](associativeFn: (T, T) => T): HasAdditionOperator[T] = new HasAdditionOperator[T] { def plus(l: T, r: T) = associativeFn(l, r) }

  /**
   * Same as v + v + v .. + v (i times in total)
   * requires i > 0, wish we had PositiveBigInt as a class
   */
  def intTimes[T](i: BigInt, v: T)(implicit sg: HasAdditionOperator[T]): T = {
    require(i > 0, "Cannot do non-positive products with a HasAdditionOperator, try Monoid/Group.intTimes")
    intTimesRec(i - 1, v, 0, (v, Vector[T]()))
  }

  @tailrec
  private def intTimesRec[T](i: BigInt, v: T, pow: Int, vaccMemo: (T, Vector[T]))(implicit sg: HasAdditionOperator[T]): T = {
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
  private def timesPow2[T](power: Int, v: T, memo: Vector[T])(implicit sg: HasAdditionOperator[T]): (T, Vector[T]) = {
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

  implicit val nullHasAdditionOperator: HasAdditionOperator[Null] = NullGroup
  implicit val unitHasAdditionOperator: HasAdditionOperator[Unit] = UnitGroup
  implicit val boolHasAdditionOperator: HasAdditionOperator[Boolean] = BooleanField
  implicit val jboolHasAdditionOperator: HasAdditionOperator[JBool] = JBoolField
  implicit val intHasAdditionOperator: HasAdditionOperator[Int] = IntRing
  implicit val jintHasAdditionOperator: HasAdditionOperator[JInt] = JIntRing
  implicit val shortHasAdditionOperator: HasAdditionOperator[Short] = ShortRing
  implicit val jshortHasAdditionOperator: HasAdditionOperator[JShort] = JShortRing
  implicit val longHasAdditionOperator: HasAdditionOperator[Long] = LongRing
  implicit val bigIntHasAdditionOperator: HasAdditionOperator[BigInt] = BigIntRing
  implicit val jlongHasAdditionOperator: HasAdditionOperator[JLong] = JLongRing
  implicit val floatHasAdditionOperator: HasAdditionOperator[Float] = FloatField
  implicit val jfloatHasAdditionOperator: HasAdditionOperator[JFloat] = JFloatField
  implicit val doubleHasAdditionOperator: HasAdditionOperator[Double] = DoubleField
  implicit val jdoubleHasAdditionOperator: HasAdditionOperator[JDouble] = JDoubleField
  implicit val stringHasAdditionOperator: HasAdditionOperator[String] = StringMonoid
  implicit def optionHasAdditionOperator[T: HasAdditionOperator]: HasAdditionOperator[Option[T]] = new OptionMonoid[T]
  implicit def listHasAdditionOperator[T]: HasAdditionOperator[List[T]] = new ListMonoid[T]
  implicit def seqHasAdditionOperator[T]: HasAdditionOperator[Seq[T]] = new SeqMonoid[T]
  implicit def indexedSeqHasAdditionOperator[T: HasAdditionOperator]: HasAdditionOperator[IndexedSeq[T]] = new IndexedSeqHasAdditionOperator[T]
  implicit def jlistHasAdditionOperator[T]: HasAdditionOperator[JList[T]] = new JListMonoid[T]
  implicit def setHasAdditionOperator[T]: HasAdditionOperator[Set[T]] = new SetMonoid[T]
  implicit def mapHasAdditionOperator[K, V: HasAdditionOperator]: HasAdditionOperator[Map[K, V]] = new MapMonoid[K, V]
  implicit def scMapHasAdditionOperator[K, V: HasAdditionOperator]: HasAdditionOperator[ScMap[K, V]] = new ScMapMonoid[K, V]
  implicit def jmapHasAdditionOperator[K, V: HasAdditionOperator]: HasAdditionOperator[JMap[K, V]] = new JMapMonoid[K, V]
  implicit def eitherHasAdditionOperator[L: HasAdditionOperator, R: HasAdditionOperator]: HasAdditionOperator[Either[L, R]] = new EitherHasAdditionOperator[L, R]
  implicit def function1HasAdditionOperator[T]: HasAdditionOperator[Function1[T, T]] = new Function1Monoid[T]
}
