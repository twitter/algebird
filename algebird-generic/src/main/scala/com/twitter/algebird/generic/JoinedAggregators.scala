package com.twitter.algebird.generic

import shapeless._
import com.twitter.algebird._

import Shapeless._

/**
 * This method allows to combine disparate aggregators that map to
 * a unified presentation type.
 *
 * This diagram illustrates the "shape" of this combinator:
 *
 *   a1 -> b1 \
 *   a2 -> b2 -> z
 *   a3 -> b3 /
 *
 * Let's use the following example code to demonstrate the usage here:
 *
 *   val a: MonoidAggregator[Dog, DogStats, Result] = ...
 *   val b: MonoidAggregator[Cat, CatStats, Result] = ...
 *   val c: MonoidAggregator[Bird, BirdStats, Result] = ...
 *   implicit val t: Semigroup[Result] = ...
 *
 * For products (things like tuples or heterogeneous lists) we can use
 * .allOf to create a new aggregator:
 *
 *   val m1: Aggregator[Dog :: Cat :: HNil, DogStats :: CatStats :: HNil, Result] =
 *     JoinedAggregators.allOf(a :: b :: HNil)
 *
 * For coproducts (things like Either or sealed traits) we can do the following:
 *
 *   val m2: MonoidAggregator[Dog :+: Cat :+: CNil, DogStats :: CatStats :: HNil, Result] =
 *     JoinedAggregators.oneOf(a :: b :: HNil)
 *
 * ...and get a single aggregator that works for dogs or cats (wrapped in a coproduct)!
 *
 * We can also say:
 *
 *   val m3: MonoidAggregator[Animal, DogStats :: CatStats :: HNil, Result] =
 *     JoinedAggregators(a :: b :: HNil).composePrepare(Generic[Animal].to)
 *
 * ...and get a single aggregator that works for all animals directly!
 *
 * You may need to fiddle with the order of your HList (e.g. a :: b ::
 * HNil) to get this compile -- it must match the order that
 * Generic[Animal].to expects.
 */
object JoinedAggregators {

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
  def oneOf[A0 <: Coproduct, A1 <: HList, Z, H <: HList](
      hlist: H)(implicit witness: OneOfEvidence[H, A0, A1, Z], z: Semigroup[Z]): MonoidAggregator[A0, A1, Z] =
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
  def allOf[A0 <: HList, A1 <: HList, Z, H <: HList](hlist: H)(
      implicit witness: AllOfEvidence[H, A0, A1, Z]): MonoidAggregator[A0, A1, Z] = witness(hlist)

  /**
   * Evidence that we can unify several aggregators into a single aggregator.
   *
   * 1. H is a heterogeneous list of aggregators.
   * 2. B0 is a coproduct of types.
   * 3. B1 is a hetergeneous list of types.
   */
  sealed abstract class OneOfEvidence[H <: HList, B0 <: Coproduct, B1 <: HList, Z] {
    def apply(h: H): MonoidAggregator[B0, B1, Z]
  }

  object OneOfEvidence {
    implicit def hsingle[A0, A1, Z]
      : OneOfEvidence[MonoidAggregator[A0, A1, Z] :: HNil, A0 :+: CNil, A1 :: HNil, Z] =
      new OneOfEvidence[MonoidAggregator[A0, A1, Z] :: HNil, A0 :+: CNil, A1 :: HNil, Z] {
        def apply(
            hlist: MonoidAggregator[A0, A1, Z] :: HNil): MonoidAggregator[A0 :+: CNil, A1 :: HNil, Z] = {
          val a = hlist.head
          new MonoidAggregator[A0 :+: CNil, A1 :: HNil, Z] {
            def prepare(input: A0 :+: CNil): A1 :: HNil = input match {
              case Inl(a0)   => a.prepare(a0) :: HNil
              case Inr(cnil) => cnil.impossible
            }
            def present(r: A1 :: HNil): Z = a.present(r.head)
            val monoid: Monoid[A1 :: HNil] = hconsMonoid(a.monoid, hnilRing)
          }
        }
      }

    implicit def cons[A0, B0 <: Coproduct, A1, B1 <: HList, Z, T <: HList](
        implicit rest: OneOfEvidence[T, B0, B1, Z],
        z: Semigroup[Z]): OneOfEvidence[MonoidAggregator[A0, A1, Z] :: T, A0 :+: B0, A1 :: B1, Z] =
      new OneOfEvidence[MonoidAggregator[A0, A1, Z] :: T, A0 :+: B0, A1 :: B1, Z] {
        def apply(hlist: MonoidAggregator[A0, A1, Z] :: T): MonoidAggregator[A0 :+: B0, A1 :: B1, Z] =
          new MonoidAggregator[A0 :+: B0, A1 :: B1, Z] {
            val a = hlist.head
            val b = rest(hlist.tail)
            def prepare(input: A0 :+: B0): A1 :: B1 = input match {
              case Inl(a0) => a.prepare(a0) :: b.monoid.zero
              case Inr(b0) => a.monoid.zero :: b.prepare(b0)
            }
            def present(r: A1 :: B1): Z =
              z.plus(a.present(r.head), b.present(r.tail))
            val monoid: Monoid[A1 :: B1] =
              hconsMonoid(a.monoid, b.monoid)
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
  sealed abstract class AllOfEvidence[H <: HList, B0 <: HList, B1 <: HList, Z] {
    def apply(h: H): MonoidAggregator[B0, B1, Z]
  }

  object AllOfEvidence {
    implicit def hsingle[A0, A1, Z]
      : AllOfEvidence[MonoidAggregator[A0, A1, Z] :: HNil, A0 :: HNil, A1 :: HNil, Z] =
      new AllOfEvidence[MonoidAggregator[A0, A1, Z] :: HNil, A0 :: HNil, A1 :: HNil, Z] {
        def apply(hlist: MonoidAggregator[A0, A1, Z] :: HNil): MonoidAggregator[A0 :: HNil, A1 :: HNil, Z] = {
          val a = hlist.head
          new MonoidAggregator[A0 :: HNil, A1 :: HNil, Z] {
            def prepare(input: A0 :: HNil): A1 :: HNil = a.prepare(input.head) :: HNil
            def present(r: A1 :: HNil): Z = a.present(r.head)
            val monoid: Monoid[A1 :: HNil] = hconsMonoid(a.monoid, hnilRing)
          }
        }
      }

    implicit def cons[A0, B0 <: HList, A1, B1 <: HList, Z, T <: HList](
        implicit rest: AllOfEvidence[T, B0, B1, Z],
        z: Semigroup[Z]): AllOfEvidence[MonoidAggregator[A0, A1, Z] :: T, A0 :: B0, A1 :: B1, Z] =
      new AllOfEvidence[MonoidAggregator[A0, A1, Z] :: T, A0 :: B0, A1 :: B1, Z] {
        def apply(hlist: MonoidAggregator[A0, A1, Z] :: T): MonoidAggregator[A0 :: B0, A1 :: B1, Z] =
          new MonoidAggregator[A0 :: B0, A1 :: B1, Z] {
            val a = hlist.head
            val b = rest(hlist.tail)
            def prepare(input: A0 :: B0): A1 :: B1 =
              a.prepare(input.head) :: b.prepare(input.tail)
            def present(r: A1 :: B1): Z =
              z.plus(a.present(r.head), b.present(r.tail))
            val monoid: Monoid[A1 :: B1] =
              hconsMonoid(a.monoid, b.monoid)
          }
      }
  }
}
