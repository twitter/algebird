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

import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalacheck.Prop.forAll

import scala.math.Equiv

/**
 * Base properties useful for all tests using Algebird's typeclasses.
 */
object BaseProperties extends MetricProperties {
  val arbReasonableBigDecimals: Arbitrary[BigDecimal] = Arbitrary(
    for {
      scale <- Gen.choose(-128, +128)
      base <- implicitly[Arbitrary[BigInt]].arbitrary
    } yield {
      (BigDecimal(base) * BigDecimal(10).pow(scale))
    })

  @deprecated(
    "Please use Equiv.universal, or the Equiv version of tests",
    since = "0.12.3")
  def defaultEq[T](t0: T, t1: T): Boolean = t0 == t1

  def approxEq(eps: Double)(f1: Double, f2: Double): Boolean =
    (scala.math.abs(f1 - f2) / scala.math.abs(f2)) < eps

  trait HigherEq[M[_]] {
    def apply[T](m: M[T], n: M[T]): Boolean
  }

  class DefaultHigherEq[M[_]] extends HigherEq[M] {
    override def apply[T](m: M[T], n: M[T]) = m == n
  }

  def isNonZero[V: Semigroup](v: V): Boolean =
    implicitly[Semigroup[V]] match {
      case mon: Monoid[_] => mon.isNonZero(v)
      case _ => true
    }

  // Associative
  def isAssociative[T: Semigroup: Arbitrary]: Prop = isAssociativeDifferentTypes[T, T]

  def isAssociativeDifferentTypes[T: Semigroup, U <: T: Arbitrary]: Prop =
    isAssociativeEquiv[T, U]

  @deprecated("use isAssociativeEquiv[T, U] with implicit Equiv[T] instance", since = "0.12.3")
  def isAssociativeEq[T: Semigroup, U <: T: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    isAssociativeEquiv[T, U]
  }

  def isAssociativeEquiv[T: Semigroup: Equiv, U <: T: Arbitrary]: Prop =
    'isAssociativeEq |: forAll { (a: U, b: U, c: U) =>
      val semi = implicitly[Semigroup[T]]
      Equiv[T].equiv(semi.plus(a, semi.plus(b, c)), semi.plus(semi.plus(a, b), c))
    }

  // Commutative
  def isCommutative[T: Semigroup: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    isCommutativeEquiv[T]
  }

  @deprecated("use isCommutativeEquiv[T] with implicit Equiv[T] instance", since = "0.12.3")
  def isCommutativeEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    isCommutativeEquiv[T]
  }

  def isCommutativeEquiv[T: Semigroup: Arbitrary: Equiv]: Prop =
    'isCommutativeEq |: forAll { (a: T, b: T) =>
      val semi = implicitly[Semigroup[T]]
      Equiv[T].equiv(semi.plus(a, b), semi.plus(b, a))
    }

  // Semigroup Laws
  def semigroupSumWorks[T: Semigroup: Arbitrary: Equiv]: Prop =
    'semigroupSumWorks |: forAll { (head: T, tail: List[T]) =>
      Equiv[T].equiv(
        Semigroup.sumOption(head :: tail).get,
        tail.foldLeft(head)(Semigroup.plus(_, _))
      )
    }

  def semigroupLaws[T: Semigroup: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    semigroupLawsEquiv[T]
  }

  @deprecated("use semigroupLawsEquiv[T] with implicit Equiv[T] instance", since = "0.12.3")
  def semigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    semigroupLawsEquiv[T]
  }

  def semigroupLawsEquiv[T: Semigroup: Arbitrary: Equiv]: Prop =
    isAssociativeEquiv[T, T] && semigroupSumWorks[T]

  // Commutative Semigroup Laws
  def commutativeSemigroupLaws[T: Semigroup: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    commutativeSemigroupLawsEquiv[T]
  }

  @deprecated("use commutativeSemigroupLawsEquiv[T] with implicit Equiv[T] instance", since = "0.12.3")
  def commutativeSemigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    commutativeSemigroupLawsEquiv[T]
  }

  def commutativeSemigroupLawsEquiv[T: Semigroup: Arbitrary: Equiv]: Prop =
    semigroupLawsEquiv[T] && isCommutativeEquiv[T]

  def isNonZeroWorksMonoid[T: Monoid: Arbitrary: Equiv]: Prop =
    'isNonZeroWorksMonoid |: forAll { (a: T, b: T) =>
      val aIsLikeZero = Monoid.zeroEquiv[T].equiv(Monoid.plus(a, b), b)
      Monoid.isNonZero(a) || aIsLikeZero
    }

  def isNonZeroWorksRing[T: Ring: Arbitrary]: Prop =
    'isNonZeroWorksRing |: forAll { (a: T, b: T) =>
      implicit val monT: Monoid[T] = implicitly[Ring[T]]
      val prodZero = !monT.isNonZero(Ring.times(a, b))
      (Monoid.isNonZero(a) && Monoid.isNonZero(b)) || prodZero
    }

  def weakZeroDifferentTypes[T: Monoid, U <: T: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    weakZeroDifferentTypesEquiv[T, U]
  }

  def weakZeroDifferentTypesEquiv[T: Monoid: Equiv, U <: T: Arbitrary]: Prop =
    'weakZeroDifferentTypes |: forAll { (a: U) =>
      val mon = implicitly[Monoid[T]]
      val zero = mon.zero
      // Some types, e.g. Maps, are not totally equal for all inputs
      // (i.e. zero values removed)
      Equiv[T].equiv(mon.plus(a, zero), mon.plus(zero, a))
    }

  def weakZero[T: Monoid: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    weakZeroEquiv[T]
  }

  def weakZeroEquiv[T: Monoid: Arbitrary: Equiv]: Prop =
    weakZeroDifferentTypesEquiv[T, T]

  def validZero[T: Monoid: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    validZeroEquiv[T]
  }

  @deprecated("use validZeroEquiv[T] with implicit Equiv[T] instance", since = "0.12.3")
  def validZeroEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    validZeroEquiv[T]
  }

  def validZeroEquiv[T: Monoid: Arbitrary: Equiv]: Prop =
    'validZeroEq |: forAll { (a: T) =>
      val mon = implicitly[Monoid[T]]
      val zero = mon.zero
      Equiv[T].equiv(a, mon.plus(a, zero)) &&
      Equiv[T].equiv(mon.plus(zero, a), a) &&
      Equiv[T].equiv(mon.plus(a, zero), mon.plus(zero, a))
    }

  def monoidLaws[T: Monoid: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    monoidLawsEquiv[T]
  }

  @deprecated("use monoidLawsEquiv[T] with implicit Equiv[T] instance", since = "0.12.3")
  def monoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    monoidLawsEquiv[T]
  }

  def monoidLawsEquiv[T: Monoid: Arbitrary: Equiv]: Prop =
    validZeroEquiv[T] && semigroupLawsEquiv[T] && isNonZeroWorksMonoid[T]

  def commutativeMonoidLaws[T: Monoid: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    commutativeMonoidLawsEquiv[T]
  }

  @deprecated("use commutativeMonoidLawsEquiv[T] with implicit Equiv[T] instance", since = "0.12.3")
  def commutativeMonoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    commutativeMonoidLawsEquiv[T]
  }

  def commutativeMonoidLawsEquiv[T: Monoid: Arbitrary: Equiv]: Prop =
    monoidLawsEquiv[T] && isCommutativeEquiv[T]

  def hasAdditiveInversesDifferentTypes[T: Group, U <: T: Arbitrary]: Prop =
    forAll { (a: U) =>
      val grp = implicitly[Group[T]]
      (!grp.isNonZero(grp.plus(grp.negate(a), a))) &&
      (!grp.isNonZero(grp.minus(a, a))) &&
      (!grp.isNonZero(grp.plus(a, grp.negate(a))))
    }

  def hasAdditiveInverses[T: Group: Arbitrary]: Prop =
    hasAdditiveInversesDifferentTypes[T, T]

  def groupLaws[T: Group: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    groupLawsEquiv[T]
  }

  @deprecated("use groupLawsEquiv[T] with implicit Equiv[T] instance", since = "0.12.3")
  def groupLawsEq[T: Group: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    groupLawsEquiv[T]
  }

  def groupLawsEquiv[T: Group: Arbitrary: Equiv]: Prop =
    monoidLawsEquiv[T] && hasAdditiveInverses[T]

  // Here are multiplicative properties:
  def validOne[T: Ring: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    validOneEquiv[T]
  }

  def validOneEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    'validOne |: forAll { (a: T) =>
      val rng = implicitly[Ring[T]]
      Equiv[T].equiv(rng.times(rng.one, a), rng.times(a, rng.one)) &&
      Equiv[T].equiv(a, rng.times(a, rng.one))
    }

  def zeroAnnihilates[T: Ring: Arbitrary]: Prop =
    'zeroAnnihilates |: forAll { (a: T) =>
      val ring = implicitly[Ring[T]]
      (!ring.isNonZero(ring.times(a, ring.zero))) &&
      (!ring.isNonZero(ring.times(ring.zero, a)))
    }

  def isDistributiveDifferentTypes[T: Ring, U <: T: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    isDistributiveDifferentTypesEquiv[T, U]
  }

  def isDistributiveDifferentTypesEquiv[T: Ring: Equiv, U <: T: Arbitrary]: Prop =
    'isDistributiveDifferentTypes |:
  forAll { (a: U, b: U, c: U) =>
    val rng = implicitly[Ring[T]]
    Equiv[T].equiv(rng.times(a, rng.plus(b, c)), rng.plus(rng.times(a, b), rng.times(a, c))) &&
    Equiv[T].equiv(rng.times(rng.plus(b, c), a), rng.plus(rng.times(b, a), rng.times(c, a)))
  }

  def isDistributive[T: Ring: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    isDistributiveEquiv[T]
  }

  def isDistributiveEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    isDistributiveDifferentTypesEquiv[T, T]

  def timesIsAssociative[T: Ring: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    timesIsAssociativeEquiv[T]
  }

  def timesIsAssociativeEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    'timesIsAssociative |: forAll { (a: T, b: T, c: T) =>
      val rng = implicitly[Ring[T]]
      Equiv[T].equiv(rng.times(a, rng.times(b, c)), rng.times(rng.times(a, b), c))
    }

  def pseudoRingLaws[T: Ring: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    pseudoRingLawsEquiv[T]
  }

  def pseudoRingLawsEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    isDistributiveEquiv[T] && timesIsAssociativeEquiv[T] && groupLawsEquiv[T] &&
  isCommutativeEquiv[T] && isNonZeroWorksRing[T]

  def semiringLaws[T: Ring: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    semiringLawsEquiv[T]
  }

  def semiringLawsEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    isDistributiveEquiv[T] && timesIsAssociativeEquiv[T] &&
      validOneEquiv[T] && commutativeMonoidLawsEquiv[T] &&
      zeroAnnihilates[T] &&
      isNonZeroWorksRing[T]

  def ringLaws[T: Ring: Arbitrary]: Prop = {
    implicit val eq: Equiv[T] = Equiv.universal
    ringLawsEquiv[T]
  }

  def ringLawsEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    validOneEquiv[T] && pseudoRingLawsEquiv[T]
}
