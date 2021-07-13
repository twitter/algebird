package com.twitter.algebird.generic

import shapeless._
import com.twitter.algebird._

import Shapeless._

/**
 * This method allows to combine disparate aggregators that share
 * a common input type and a semigroup on the common output type
 *
 * This diagram illustrates the "shape" of this combinator:
 *
 *       / b1 \
 *   a -> b2 -> c
 *       \ b3 /
 *
 * This is not as common as ApplicativeAggregators or ParallelAggregators,
 * but could arise if for instance you wanted to evaluate a weighted threshold:
 * `b1, b2, ...` might be the individual feature values, then the functions
 * `b1 => c` could be weighting the feature into the return space. We then
 * sum the weights (this would be something like a linear of logistic regression).
 *
 * Let's use the following example code to demonstrate the usage here:
 *
 *   val a: MonoidAggregator[Animal, ColorStats, Result] = ...
 *   val b: MonoidAggregator[Animal, ShapeStats, Result] = ...
 *
 *   val m1: Aggregator[Animal, ColorStats :: ShapeStats :: HNil, Result] =
 *     CombinedAggregators(a :: b :: HNil)
 *
 *  The name comes from the fact that "combine" is sometimes used as the name
 *  for general semigroups.
 *
 */
object CombinedAggregators {

  def apply[A0, A1 <: HList, A2, H <: HList](hlist: H)(
      implicit witness: Evidence[H, A0, A1, A2]): MonoidAggregator[A0, A1, A2] = witness(hlist)

  /**
   * Types like this in type-level programming are often called "evidence" since
   * they are evidence of some structure, but not used in the input or the output.
   */
  sealed abstract class Evidence[H <: HList, B0, B1 <: HList, B2] {
    def apply(h: H): MonoidAggregator[B0, B1, B2]
  }

  object Evidence {
    implicit def hsingle[A0, A1, A2]: Evidence[MonoidAggregator[A0, A1, A2] :: HNil, A0, A1 :: HNil, A2] =
      new Evidence[MonoidAggregator[A0, A1, A2] :: HNil, A0, A1 :: HNil, A2] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: HNil): MonoidAggregator[A0, A1 :: HNil, A2] = {
          val a = hlist.head
          new MonoidAggregator[A0, A1 :: HNil, A2] {
            def prepare(input: A0): A1 :: HNil = a.prepare(input) :: HNil
            def present(r: A1 :: HNil): A2 = a.present(r.head)
            val monoid: Monoid[A1 :: HNil] = hconsMonoid(a.monoid, hnilRing)
          }
        }
      }

    implicit def cons[A0, A1, B1 <: HList, A2, T <: HList](
        implicit rest: Evidence[T, A0, B1, A2],
        z: Semigroup[A2]): Evidence[MonoidAggregator[A0, A1, A2] :: T, A0, A1 :: B1, A2] =
      new Evidence[MonoidAggregator[A0, A1, A2] :: T, A0, A1 :: B1, A2] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: T): MonoidAggregator[A0, A1 :: B1, A2] =
          new MonoidAggregator[A0, A1 :: B1, A2] {
            val a = hlist.head
            val b = rest(hlist.tail)
            def prepare(input: A0): A1 :: B1 = a.prepare(input) :: b.prepare(input)
            def present(r: A1 :: B1): A2 = z.plus(a.present(r.head), b.present(r.tail))
            val monoid: Monoid[A1 :: B1] = hconsMonoid(a.monoid, b.monoid)
          }
      }
  }
}
