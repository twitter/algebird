package com.twitter.algebird.generic

import shapeless._

object EquivOrdering extends EquivOrdering1 {
  implicit def hconsOrdering[A, B <: HList](
      implicit
      a: Ordering[A],
      lb: Lazy[Ordering[B]]
  ): Ordering[A :: B] =
    new Ordering[A :: B] {
      val b = lb.value
      def compare(x: A :: B, y: A :: B): Int = {
        val c = a.compare(x.head, y.head)
        if (c == 0) b.compare(x.tail, y.tail) else c
      }
    }

  implicit val hnilOrdering: Ordering[HNil] =
    new Ordering[HNil] {
      def compare(x: HNil, y: HNil): Int = 0
      override def equiv(a: HNil, b: HNil) = true
    }

  /**
   * this is intentionally not implicit to avoid superceding the instance that may be
   * set up in a companion
   *
   * use it with implicit val myOrd: Ordering[MyType] = genericOrdering
   */
  def genericOrdering[A, Repr](implicit gen: Generic.Aux[A, Repr], r: Ordering[Repr]): Ordering[A] =
    r.on(gen.to _)
}

abstract class EquivOrdering1 {

  implicit def hconsEquiv[A, B <: HList](implicit a: Equiv[A], lb: Lazy[Equiv[B]]): Equiv[A :: B] =
    new Equiv[A :: B] {

      val b = lb.value
      def equiv(x: A :: B, y: A :: B): Boolean =
        a.equiv(x.head, y.head) && b.equiv(x.tail, y.tail)
    }

  /**
   * this is intentionally not implicit to avoid superceding the instance that may be
   * set up in a companion
   *
   * use it with implicit val myEqv: Equiv[MyType] = genericEquiv
   */
  def genericEquiv[A, Repr](implicit gen: Generic.Aux[A, Repr], r: Equiv[Repr]): Equiv[A] =
    new Equiv[A] {
      def equiv(left: A, right: A) = r.equiv(gen.to(left), gen.to(right))
    }
}
