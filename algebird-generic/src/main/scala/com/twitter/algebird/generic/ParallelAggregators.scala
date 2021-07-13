package com.twitter.algebird.generic

import shapeless._
import com.twitter.algebird._

import Shapeless._

/**
 *
 *   a1 -> b1 -> z1
 *   a2 -> b2 -> z2
 *   a3 -> b3 -> z3
 *
 * ParallelAggregators is the generic version of Aggregator.zip it assumes
 * an input of an HList, and then just breaks the inputs up and applies each
 * aggregator in parallel.
 *
 *   val a: MonoidAggregator[Dog, DogStats, DogR] = ...
 *   val b: MonoidAggregator[Cat, CatStats, CatR] = ...
 *   val c: MonoidAggregator[Bird, BirdStats, BirdR] = ...
 *
 * Then you can say:
 *
 *   ParallelAggregators(a :: b :: c :: HNil)
 *
 * and get a single aggregator that works for dogs, cats, and birds.
 *
 * (of type MonoidAggregator[Dog :: Cat :: Bird :: HNil, DogStats :: CatStats :: BirdStats :: HNil, DogR :: CatR :: BirdR :: HNil])
 */
object ParallelAggregators {

  /**
   * This is when we have a union/sealed-trait input type and we
   * have aggregators for branches of the union.
   *
   * We use Coproduct, which in shapeless is generalization of
   * Either (similar to how HList is a generalization of Tuple2).
   *
   * To create an instance of `A :+: B :+: CNil` you do:
   * `shapeless.Inl(a)` or `shapeless.Inr(shapeless.Inl(b))~ (CNil has
   * no actual instance and is like another name for Nothing in
   * the same way that HNil is like another name for Unit.
   *
   * typically, we expect you to use `shapeless.Generic[T].to` to create
   * these from sealed traits.
   *
   */
  def oneOf[A0 <: Coproduct, A1 <: HList, A2 <: HList, H <: HList](hlist: H)(
      implicit witness: OneOfEvidence[H, A0, A1, A2]): MonoidAggregator[A0, A1, A2] =
    witness(hlist)

  /**
   * This is when we have several values coming in at the same time.
   * You can construct an HList from a tuple with:
   *
   *   import shapeless._
   *   import syntax.std.product._
   *
   *   t.productElements
   */
  def allOf[A0 <: HList, A1 <: HList, A2 <: HList, H <: HList](hlist: H)(
      implicit witness: AllOfEvidence[H, A0, A1, A2]): MonoidAggregator[A0, A1, A2] =
    witness(hlist)

  sealed abstract class OneOfEvidence[H <: HList, B0, B1, B2] {
    def apply(h: H): MonoidAggregator[B0, B1, B2]
  }

  object OneOfEvidence {
    implicit def hsingle[A0, A1, A2]
      : OneOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :+: CNil, A1 :: HNil, A2 :: HNil] =
      new OneOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :+: CNil, A1 :: HNil, A2 :: HNil] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: HNil)
          : MonoidAggregator[A0 :+: CNil, A1 :: HNil, A2 :: HNil] = {
          val a = hlist.head
          new MonoidAggregator[A0 :+: CNil, A1 :: HNil, A2 :: HNil] {
            def prepare(input: A0 :+: CNil): A1 :: HNil = input match {
              case Inl(a0)   => a.prepare(a0) :: HNil
              case Inr(cnil) => cnil.impossible
            }
            def present(r: A1 :: HNil): A2 :: HNil = a.present(r.head) :: HNil
            val monoid: Monoid[A1 :: HNil] = hconsMonoid(a.monoid, hnilRing)
          }
        }
      }

    implicit def cons[A0, B0 <: Coproduct, A1, B1 <: HList, A2, B2 <: HList, T <: HList](
        implicit rest: OneOfEvidence[T, B0, B1, B2])
      : OneOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :+: B0, A1 :: B1, A2 :: B2] =
      new OneOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :+: B0, A1 :: B1, A2 :: B2] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: T): MonoidAggregator[A0 :+: B0, A1 :: B1, A2 :: B2] =
          new MonoidAggregator[A0 :+: B0, A1 :: B1, A2 :: B2] {
            val a = hlist.head
            val b = rest(hlist.tail)
            def prepare(input: A0 :+: B0): A1 :: B1 =
              input match {
                case Inl(a0) => a.prepare(a0) :: b.monoid.zero
                case Inr(b0) => a.monoid.zero :: b.prepare(b0)
              }
            def present(r: A1 :: B1): A2 :: B2 = a.present(r.head) :: b.present(r.tail)
            val monoid: Monoid[A1 :: B1] = hconsMonoid(a.monoid, b.monoid)
          }
      }
  }

  sealed abstract class AllOfEvidence[H <: HList, B0, B1, B2] {
    def apply(h: H): MonoidAggregator[B0, B1, B2]
  }

  object AllOfEvidence {
    implicit def hsingle[A0, A1, A2]
      : AllOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :: HNil, A1 :: HNil, A2 :: HNil] =
      new AllOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :: HNil, A1 :: HNil, A2 :: HNil] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: HNil)
          : MonoidAggregator[A0 :: HNil, A1 :: HNil, A2 :: HNil] = {
          val a = hlist.head
          new MonoidAggregator[A0 :: HNil, A1 :: HNil, A2 :: HNil] {
            def prepare(input: A0 :: HNil): A1 :: HNil = a.prepare(input.head) :: HNil
            def present(r: A1 :: HNil): A2 :: HNil = a.present(r.head) :: HNil
            val monoid: Monoid[A1 :: HNil] = hconsMonoid(a.monoid, hnilRing)
          }
        }
      }

    implicit def cons[A0, B0 <: HList, A1, B1 <: HList, A2, B2 <: HList, T <: HList](
        implicit rest: AllOfEvidence[T, B0, B1, B2])
      : AllOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :: B0, A1 :: B1, A2 :: B2] =
      new AllOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :: B0, A1 :: B1, A2 :: B2] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: T): MonoidAggregator[A0 :: B0, A1 :: B1, A2 :: B2] =
          new MonoidAggregator[A0 :: B0, A1 :: B1, A2 :: B2] {
            val a = hlist.head
            val b = rest(hlist.tail)
            def prepare(input: A0 :: B0): A1 :: B1 =
              a.prepare(input.head) :: b.prepare(input.tail)
            def present(r: A1 :: B1): A2 :: B2 = a.present(r.head) :: b.present(r.tail)
            val monoid: Monoid[A1 :: B1] = hconsMonoid(a.monoid, b.monoid)
          }
      }
  }
}
