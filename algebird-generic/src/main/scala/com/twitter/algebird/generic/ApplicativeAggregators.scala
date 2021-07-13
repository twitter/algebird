package com.twitter.algebird.generic

import shapeless._
import com.twitter.algebird._

import Shapeless._

/**
 * This method allows to combine disparate aggregators that share
 * a common input type.
 *
 * This diagram illustrates the "shape" of this combinator:
 *
 *      / b1 -> z1
 *  a1 -> b2 -> z2
 *      \ b3 -> z3
 *
 * Let's use the following example code to demonstrate the usage here:
 *
 *   val a: MonoidAggregator[Animal, ColorStats, ColorResult] = ...
 *   val b: MonoidAggregator[Animal, ShapeStats, ShapeResult] = ...
 *
 *   val m1: Aggregator[Animal, ColorStats :: ShapeStats :: HNil, ColorResult :: ShapeResult :: HNil] =
 *     ApplicativeAggregators(a :: b :: HNil)
 *
 *  The name comes from the fact that this is the standard "Applicative"
 *  product operation (which algebird calls .join). For non-monoid Aggregators
 *  algebird GeneratedTupleAggregator.fromN functions do the same thing
 */
object ApplicativeAggregators {

  def apply[A0, A1 <: HList, A2 <: HList, H <: HList](hlist: H)(
      implicit witness: Evidence[H, A0, A1, A2]): MonoidAggregator[A0, A1, A2] = witness(hlist)

  /**
   * Types like this in type-level programming are often called "evidence" since
   * they are evidence of some structure, but not used in the input or the output.
   */
  sealed abstract class Evidence[H <: HList, B0, B1 <: HList, B2 <: HList] {
    def apply(h: H): MonoidAggregator[B0, B1, B2]
  }

  object Evidence {
    implicit def hsingle[A0, A1, A2]
      : Evidence[MonoidAggregator[A0, A1, A2] :: HNil, A0, A1 :: HNil, A2 :: HNil] =
      new Evidence[MonoidAggregator[A0, A1, A2] :: HNil, A0, A1 :: HNil, A2 :: HNil] {
        def apply(
            hlist: MonoidAggregator[A0, A1, A2] :: HNil): MonoidAggregator[A0, A1 :: HNil, A2 :: HNil] = {
          val a = hlist.head
          new MonoidAggregator[A0, A1 :: HNil, A2 :: HNil] {
            def prepare(input: A0): A1 :: HNil = a.prepare(input) :: HNil
            def present(r: A1 :: HNil): A2 :: HNil = a.present(r.head) :: HNil
            val monoid: Monoid[A1 :: HNil] = hconsMonoid(a.monoid, hnilRing)
          }
        }
      }

    implicit def cons[A0, A1, B1 <: HList, A2, B2 <: HList, T <: HList](
        implicit rest: Evidence[T, A0, B1, B2])
      : Evidence[MonoidAggregator[A0, A1, A2] :: T, A0, A1 :: B1, A2 :: B2] =
      new Evidence[MonoidAggregator[A0, A1, A2] :: T, A0, A1 :: B1, A2 :: B2] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: T): MonoidAggregator[A0, A1 :: B1, A2 :: B2] =
          new MonoidAggregator[A0, A1 :: B1, A2 :: B2] {
            val a = hlist.head
            val b = rest(hlist.tail)
            def prepare(input: A0): A1 :: B1 = a.prepare(input) :: b.prepare(input)
            def present(r: A1 :: B1): A2 :: B2 = a.present(r.head) :: b.present(r.tail)
            val monoid: Monoid[A1 :: B1] = hconsMonoid(a.monoid, b.monoid)
          }
      }
  }
}
