package com.stripe.zoolander.util.algebird

import shapeless._
import shapeless.ops.hlist.ToList
import com.twitter.algebird._

object Shapeless extends Shapeless1 {

  implicit def hconsOrdering[A, B <: HList](implicit la: Lazy[Ordering[A]],
                                            b: Ordering[B]): Ordering[A :: B] =
    new Ordering[A :: B] {
      val a = la.value
      def compare(x: A :: B, y: A :: B): Int = {
        val c = a.compare(x.head, y.head)
        if (c == 0) b.compare(x.tail, y.tail) else c
      }
    }

  implicit val hnilOrdering: Ordering[HNil] =
    new Ordering[HNil] {
      def compare(x: HNil, y: HNil): Int = 0
    }

  /**
   * Pairwise monoid for arbitrary heterogenuous lists (HList).
   */
  implicit def hconsMonoid[A, B <: HList](implicit la: Lazy[Monoid[A]], b: Monoid[B]): Monoid[A :: B] = {
    // We use Lazy[Monoid[A]] to avoid bogus ambiguous implicits at
    // the type-level. There is no value-level laziness needed, so we
    // immediately evaluate la.value.
    val a = la.value
    new HConsSemigroup[A, B](a, b) with Monoid[A :: B] {
      def zero: A :: B = a.zero :: b.zero
    }
  }

  implicit val hnilMonoid: Monoid[HNil] =
    new Monoid[HNil] {
      def zero: HNil = HNil
      def plus(x: HNil, y: HNil): HNil = HNil
      override def sumOption(xs: TraversableOnce[HNil]): Option[HNil] =
        if (xs.isEmpty) None else Some(HNil)
    }

  object HListSum {
    def sum[L <: HList, T](hlist: L)(implicit toList: ToList[L, T], m: Monoid[T]): T =
      m.sum(toList(hlist))

    def sum1[L <: HList, T](cons: T :: L)(implicit toList: ToList[L, T], s: Semigroup[T]): T =
      // this get can never fail because there is at least one item
      s.sumOption(cons.head :: toList(cons.tail)).get
  }
}

abstract class Shapeless1 {

  implicit def hconsEquiv[A, B <: HList](implicit la: Lazy[Equiv[A]], b: Equiv[B]): Equiv[A :: B] =
    new Equiv[A :: B] {
      val a = la.value
      def equiv(x: A :: B, y: A :: B): Boolean =
        a.equiv(x.head, y.head) && b.equiv(x.tail, y.tail)
    }

  /**
   * Pairwise monoid for arbitrary heterogenuous lists (HList).
   */
  implicit def hconsSemigroup[A, B <: HList](implicit la: Lazy[Semigroup[A]],
                                             b: Semigroup[B]): Semigroup[A :: B] =
    new HConsSemigroup[A, B](la.value, b)

  class HConsSemigroup[A, B <: HList](a: Semigroup[A], b: Semigroup[B]) extends Semigroup[A :: B] {

    // remove this in favor of ArrayBufferedOperation.fromSumOption
    // once we're on an algebird version that includes this method.
    def fromSumOption[T](size: Int)(implicit m: Semigroup[T]): BufferedReduce[T] =
      new ArrayBufferedOperation[T, T](size) with BufferedReduce[T] {
        // ts guaranteed to be non-empty
        def operate(ts: Seq[T]): T = m.sumOption(ts.iterator).get
      }

    def plus(x: A :: B, y: A :: B): A :: B =
      a.plus(x.head, y.head) :: b.plus(x.tail, y.tail)
    override def sumOption(xs: TraversableOnce[A :: B]): Option[A :: B] =
      if (xs.isEmpty) {
        None
      } else {
        val bufA = fromSumOption[A](1000)(a)
        val bufB = fromSumOption[B](1000)(b)
        xs.foreach {
          case a0 :: b0 =>
            bufA.put(a0)
            bufB.put(b0)
        }
        Some(bufA.flush.get :: bufB.flush.get)
      }
  }
}
