package com.twitter.algebird.generic

import shapeless._
import com.twitter.algebird._

/**
 * This method allows to combine disparate aggregators that share
 * a common middle aggregation type (and Monoid, which we assume
 * to be same).
 *
 * This diagram illustrates the "shape" of this combinator:
 *
 *  a1 \      / z1
 *  a2 -> b* -> z2
 *  a3 /      \ z3
 *
 * Cinched may be useful in cases where the B type is something like
 * Moments, where we want to compute things like Z-score, mean,
 * std-dev, etc. from moments.
 *
 * Let's use the following example code to demonstrate the usage
 * here:
 *
 *   val a: MonoidAggregator[Dog, Stats, DogResult] = ...
 *   val b: MonoidAggregator[Cat, Stats, CatResult] = ...
 *
 * For products (things like tuples or heterogeneous lists) we use
 * .allOf to create a new aggregator:
 *
 *   val m1: Aggregator[Dog :: Cat :: HNil, Stats, DogResult :: CatResult :: HNil] =
 *     CinchedAggregators.allOf(a :: b :: HNil)
 *
 * For coproducts (things like Either or sealed traits) we can do the following:
 *
 *   val m2: MonoidAggregator[Dog :+: Cat :+: CNil, Stats, DogResult :: CatResult :: HNil] =
 *     CinchedAggregators.oneOf(a :: b :: HNil)
 *
 * ...and get a single aggregator that works for dogs, cats, and birds (wrapped in a coproduct)!
 *
 * We can also say:
 *
 *   val m2: MonoidAggregator[Animal, Stats, DogResult :: CatResult :: HNil] =
 *     CinchedAggregators.oneOf(a :: b :: HNil).composePrepare(Generic[Animal].to)
 *
 * ...and get a single aggregator that works for all animals directly!
 *
 * You may need to fiddle with the order of your HList (e.g. a :: b ::
 * HNil) to get this compile -- it must match the order that
 * Generic[Animal].to expects.
 */
object CinchedAggregators {

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
  def oneOf[A0 <: Coproduct, A1, A2 <: HList, H <: HList](hlist: H)(
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
  def allOf[A0 <: HList, A1, A2 <: HList, H <: HList](hlist: H)(
      implicit witness: AllOfEvidence[H, A0, A1, A2]): MonoidAggregator[A0, A1, A2] =
    witness(hlist)

// CinchedAggregators
// product: aggregators give us semigroup[b] (*require coherence)
// coproduct: we're fine

  /**
   * Evidence that we can unify several aggregators into a single aggregator.
   *
   * 1. H is a heterogeneous list of aggregators.
   * 2. B0 is a coproduct of types.
   * 3. B1 is a hetergeneous list of types.
   */
  sealed abstract class OneOfEvidence[H <: HList, B0 <: Coproduct, B1, B2 <: HList] {
    def apply(h: H): MonoidAggregator[B0, B1, B2]
  }

  object OneOfEvidence {
    implicit def hsingle[A0, A1, A2]
      : OneOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :+: CNil, A1, A2 :: HNil] =
      new OneOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :+: CNil, A1, A2 :: HNil] {
        def apply(
            hlist: MonoidAggregator[A0, A1, A2] :: HNil): MonoidAggregator[A0 :+: CNil, A1, A2 :: HNil] = {
          val a = hlist.head
          new MonoidAggregator[A0 :+: CNil, A1, A2 :: HNil] {
            def prepare(input: A0 :+: CNil): A1 = input match {
              case Inl(a0)   => a.prepare(a0)
              case Inr(cnil) => cnil.impossible
            }
            def present(r: A1): A2 :: HNil = a.present(r) :: HNil
            def monoid: Monoid[A1] = a.monoid
          }
        }
      }

    implicit def cons[A0, B0 <: Coproduct, A1, A2, B2 <: HList, T <: HList](
        implicit rest: OneOfEvidence[T, B0, A1, B2])
      : OneOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :+: B0, A1, A2 :: B2] =
      new OneOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :+: B0, A1, A2 :: B2] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: T): MonoidAggregator[A0 :+: B0, A1, A2 :: B2] =
          new MonoidAggregator[A0 :+: B0, A1, A2 :: B2] {
            val a = hlist.head
            val b = rest(hlist.tail)
            def prepare(input: A0 :+: B0): A1 = input match {
              case Inl(a0) => a.prepare(a0)
              case Inr(b0) => b.prepare(b0)
            }
            def present(r: A1): A2 :: B2 =
              a.present(r) :: b.present(r)
            def monoid: Monoid[A1] =
              a.monoid // assume a.monoid == b.monoid
          }
      }
  }

  /**
   * Evidence that we can unify several aggregators into a single aggregator.
   *
   * 1. H is a heterogeneous list of aggregators.
   * 2. B0 is a heterogeneous list of types.
   * 3. B1 is a heterogeneous list of types.
   */
  sealed abstract class AllOfEvidence[H <: HList, B0 <: HList, B1, B2 <: HList] {
    def apply(h: H): MonoidAggregator[B0, B1, B2]
  }

  object AllOfEvidence {
    implicit def hsingle[A0, A1, A2]
      : AllOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :: HNil, A1, A2 :: HNil] =
      new AllOfEvidence[MonoidAggregator[A0, A1, A2] :: HNil, A0 :: HNil, A1, A2 :: HNil] {
        def apply(
            hlist: MonoidAggregator[A0, A1, A2] :: HNil): MonoidAggregator[A0 :: HNil, A1, A2 :: HNil] = {
          val a = hlist.head
          new MonoidAggregator[A0 :: HNil, A1, A2 :: HNil] {
            def prepare(input: A0 :: HNil): A1 = a.prepare(input.head)
            def present(r: A1): A2 :: HNil = a.present(r) :: HNil
            def monoid: Monoid[A1] = a.monoid
          }
        }
      }

    implicit def cons[A0, B0 <: HList, A1, A2, B2 <: HList, T <: HList](
        implicit rest: AllOfEvidence[T, B0, A1, B2])
      : AllOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :: B0, A1, A2 :: B2] =
      new AllOfEvidence[MonoidAggregator[A0, A1, A2] :: T, A0 :: B0, A1, A2 :: B2] {
        def apply(hlist: MonoidAggregator[A0, A1, A2] :: T): MonoidAggregator[A0 :: B0, A1, A2 :: B2] =
          new MonoidAggregator[A0 :: B0, A1, A2 :: B2] {
            val a = hlist.head
            val b = rest(hlist.tail)
            // assume a.monoid == b.monoid
            def prepare(input: A0 :: B0): A1 =
              a.monoid.plus(a.prepare(input.head), b.prepare(input.tail))
            def present(r: A1): A2 :: B2 =
              a.present(r) :: b.present(r)
            def monoid: Monoid[A1] =
              a.monoid
          }
      }
  }
}
