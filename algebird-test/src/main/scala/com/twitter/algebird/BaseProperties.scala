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
object BaseProperties extends MetricProperties with BasePropertiesCompat {

  /**
   * We generate a restricted set of BigDecimals for our tests because if we use the full range then the way
   * we lose precision in addition does not satisfy the distributive property perfectly. This means BigDecimal
   * isn't truly a Ring under it's very strict Equiv.
   */
  val arbReasonableBigDecimals: Arbitrary[BigDecimal] = Arbitrary(for {
    scale <- Gen.choose(-7, +7)
    base <- implicitly[Arbitrary[Int]].arbitrary
  } yield {
    BigDecimal(base) * BigDecimal(10).pow(scale)
  })

  // $COVERAGE-OFF$Turn off coverage for deprecated laws.
  @deprecated("Please use Equiv.universal, or the Equiv version of tests", since = "0.12.3")
  def defaultEq[T](t0: T, t1: T): Boolean = t0 == t1

  // Associative
  @deprecated("use isAssociativeDifferentTypes[T, U] with implicit Equiv[T] instance", since = "0.13.0")
  def isAssociativeEq[T: Semigroup, U <: T: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    isAssociativeDifferentTypes[T, U]
  }

  @deprecated("use isAssociativeDifferentTypes[T, U]", since = "0.13.0")
  def isAssociativeEquiv[T: Semigroup: Equiv, U <: T: Arbitrary]: Prop =
    isAssociativeDifferentTypes[T, U]

  @deprecated("use isCommutative[T] with implicit Equiv[T] instance", since = "0.13.0")
  def isCommutativeEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    isCommutative[T]
  }

  @deprecated("use isCommutative[T]", since = "0.13.0")
  def isCommutativeEquiv[T: Semigroup: Arbitrary: Equiv]: Prop =
    isCommutative[T]

  @deprecated("use semigroupLaws[T] with implicit Equiv[T] instance", since = "0.13.0")
  def semigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    semigroupLaws[T]
  }

  @deprecated("use semigroupLaws[T]", since = "0.13.0")
  def semigroupLawsEquiv[T: Semigroup: Arbitrary: Equiv]: Prop =
    semigroupLaws[T]

  @deprecated("use commutativeSemigroupLaws[T] with implicit Equiv[T] instance", since = "0.13.0")
  def commutativeSemigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    commutativeSemigroupLaws[T]
  }

  @deprecated("use commutativeSemigroupLaws[T]", since = "0.13.0")
  def commutativeSemigroupLawsEquiv[T: Semigroup: Arbitrary: Equiv]: Prop =
    commutativeSemigroupLaws[T]

  @deprecated("use weakZeroDifferentTypes[T, U]", since = "0.13.0")
  def weakZeroDifferentTypesEquiv[T: Monoid: Equiv, U <: T: Arbitrary]: Prop =
    weakZeroDifferentTypes[T, U]

  @deprecated("use weakZero[T]", since = "0.13.0")
  def weakZeroEquiv[T: Monoid: Arbitrary: Equiv]: Prop = weakZero[T]

  @deprecated("use validZero[T] with implicit Equiv[T] instance", since = "0.13.0")
  def validZeroEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    validZero[T]
  }

  @deprecated("use validZero[T]", since = "0.13.0")
  def validZeroEquiv[T: Monoid: Arbitrary: Equiv]: Prop = validZero[T]

  @deprecated("use monoidLaws[T] with implicit Equiv[T] instance", since = "0.13.0")
  def monoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    monoidLaws[T]
  }

  @deprecated("use monoidLaws[T]", since = "0.13.0")
  def monoidLawsEquiv[T: Monoid: Arbitrary: Equiv]: Prop = monoidLaws[T]

  @deprecated("use commutativeMonoidLaws[T] with implicit Equiv[T] instance", since = "0.13.0")
  def commutativeMonoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    commutativeMonoidLaws[T]
  }

  @deprecated("use commutativeMonoidLaws[T]", since = "0.13.0")
  def commutativeMonoidLawsEquiv[T: Monoid: Arbitrary: Equiv]: Prop =
    commutativeMonoidLaws[T]

  @deprecated("use groupLaws[T] with implicit Equiv[T] instance", since = "0.13.0")
  def groupLawsEq[T: Group: Arbitrary](eqfn: (T, T) => Boolean): Prop = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    groupLaws[T]
  }

  @deprecated("use groupLaws[T]", since = "0.13.0")
  def groupLawsEquiv[T: Group: Arbitrary: Equiv]: Prop =
    groupLaws[T]

  @deprecated("use validOne[T]", since = "0.13.0")
  def validOneEquiv[T: Ring: Arbitrary: Equiv]: Prop = validOne[T]

  @deprecated("use isDistributiveDifferentTypes[T, U]", since = "0.13.0")
  def isDistributiveDifferentTypesEquiv[T: Ring: Equiv, U <: T: Arbitrary]: Prop =
    isDistributiveDifferentTypes[T, U]

  @deprecated("use isDistributive[T]", since = "0.13.0")
  def isDistributiveEquiv[T: Ring: Arbitrary: Equiv]: Prop = isDistributive[T]

  @deprecated("use timesIsAssociative[T]", since = "0.13.0")
  def timesIsAssociativeEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    timesIsAssociative[T]

  @deprecated("use pseudoRingLaws[T]", since = "0.13.0")
  def pseudoRingLawsEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    pseudoRingLaws[T]

  @deprecated("use semiringLaws[T]", since = "0.13.0")
  def semiringLawsEquiv[T: Ring: Arbitrary: Equiv]: Prop =
    semiringLaws[T]

  @deprecated("use ringLaws[T]", since = "0.13.0")
  def ringLawsEquiv[T: Ring: Arbitrary: Equiv]: Prop = ringLaws[T]
  // $COVERAGE-ON$

  def approxEq(eps: Double)(f1: Double, f2: Double): Boolean =
    if (f2 == 0)
      scala.math.abs(f1) < eps
    else (scala.math.abs(f1 - f2) / scala.math.abs(f2)) < eps

  def approxEqOrBothNaN(eps: Double)(f1: Double, f2: Double): Boolean =
    (f1.isNaN && f2.isNaN) || f1 == f2 || approxEq(eps)(f1, f2)

  trait HigherEq[M[_]] {
    def apply[T](m: M[T], n: M[T]): Boolean
  }

  class DefaultHigherEq[M[_]] extends HigherEq[M] {
    override def apply[T](m: M[T], n: M[T]): Boolean = m == n
  }


  def isAssociativeDifferentTypes[T: Semigroup: Equiv, U <: T: Arbitrary]: Prop =
    "isAssociativeEq" |: forAll { (a: U, b: U, c: U) =>
      val semi = implicitly[Semigroup[T]]
      Equiv[T].equiv(semi.plus(a, semi.plus(b, c)), semi.plus(semi.plus(a, b), c))
    }

  def isAssociative[T: Semigroup: Arbitrary: Equiv]: Prop =
    isAssociativeDifferentTypes[T, T]

  // Commutative
  def isCommutative[T: Semigroup: Arbitrary: Equiv]: Prop =
    "isCommutativeEq" |: forAll { (a: T, b: T) =>
      val semi = implicitly[Semigroup[T]]
      Equiv[T].equiv(semi.plus(a, b), semi.plus(b, a))
    }

  // Semigroup Laws
  def semigroupSumWorks[T: Semigroup: Arbitrary: Equiv]: Prop =
    "semigroupSumWorks" |: forAll { (head: T, tail: List[T]) =>
      Equiv[T].equiv(Semigroup.sumOption(head :: tail).get, tail.foldLeft(head)(Semigroup.plus(_, _)))
    }

  def semigroupLaws[T: Semigroup: Arbitrary: Equiv]: Prop =
    isAssociative[T] && semigroupSumWorks[T]

  // Commutative Semigroup Laws
  def commutativeSemigroupLaws[T: Semigroup: Arbitrary: Equiv]: Prop =
    semigroupLaws[T] && isCommutative[T]

  def isNonZeroWorksMonoid[T: Monoid: Arbitrary: Equiv]: Prop =
    "isNonZeroWorksMonoid" |: forAll { (a: T, b: T) =>
      val aIsLikeZero = Monoid.zeroEquiv[T].equiv(Monoid.plus(a, b), b)
      Monoid.isNonZero(a) || aIsLikeZero
    }

  def isNonZeroWorksRing[T: Ring: Arbitrary]: Prop =
    "isNonZeroWorksRing" |: forAll { (a: T, b: T) =>
      implicit val monT: Monoid[T] = implicitly[Ring[T]]
      val prodZero = !monT.isNonZero(Ring.times(a, b))
      (Monoid.isNonZero(a) && Monoid.isNonZero(b)) || prodZero
    }

  def weakZeroDifferentTypes[T: Monoid: Equiv, U <: T: Arbitrary]: Prop =
    "weakZeroDifferentTypes" |: forAll { (a: U) =>
      val mon = implicitly[Monoid[T]]
      val zero = mon.zero
      // Some types, e.g. Maps, are not totally equal for all inputs
      // (i.e. zero values removed)
      Equiv[T].equiv(mon.plus(a, zero), mon.plus(zero, a))
    }

  def weakZero[T: Monoid: Arbitrary: Equiv]: Prop =
    weakZeroDifferentTypes[T, T]

  def validZero[T: Monoid: Arbitrary: Equiv]: Prop =
    "validZeroEq" |: forAll { (a: T) =>
      val mon = implicitly[Monoid[T]]
      val zero = mon.zero
      Equiv[T].equiv(a, mon.plus(a, zero)) &&
      Equiv[T].equiv(mon.plus(zero, a), a) &&
      Equiv[T].equiv(mon.plus(a, zero), mon.plus(zero, a))
    }

  def monoidLaws[T: Monoid: Arbitrary: Equiv]: Prop =
    validZero[T] && semigroupLaws[T] && isNonZeroWorksMonoid[T]

  def commutativeMonoidLaws[T: Monoid: Arbitrary: Equiv]: Prop =
    monoidLaws[T] && isCommutative[T]

  def hasAdditiveInversesDifferentTypes[T: Group, U <: T: Arbitrary]: Prop =
    forAll { (a: U) =>
      val grp = implicitly[Group[T]]
      (!grp.isNonZero(grp.plus(grp.negate(a), a))) &&
      (!grp.isNonZero(grp.minus(a, a))) &&
      (!grp.isNonZero(grp.plus(a, grp.negate(a))))
    }

  def hasAdditiveInverses[T: Group: Arbitrary]: Prop =
    hasAdditiveInversesDifferentTypes[T, T]

  def groupLaws[T: Group: Arbitrary: Equiv]: Prop =
    monoidLaws[T] && hasAdditiveInverses[T]

  // Here are multiplicative properties:
  def validOne[T: Ring: Arbitrary: Equiv]: Prop =
    "validOne" |: forAll { (a: T) =>
      val rng = implicitly[Ring[T]]
      Equiv[T].equiv(rng.times(rng.one, a), rng.times(a, rng.one)) &&
      Equiv[T].equiv(a, rng.times(a, rng.one))
    }

  def zeroAnnihilates[T: Ring: Arbitrary]: Prop =
    "zeroAnnihilates" |: forAll { (a: T) =>
      val ring = implicitly[Ring[T]]
      (!ring.isNonZero(ring.times(a, ring.zero))) &&
      (!ring.isNonZero(ring.times(ring.zero, a)))
    }

  def isDistributiveDifferentTypes[T: Ring: Equiv, U <: T: Arbitrary]: Prop =
    "isDistributiveDifferentTypes" |:
      forAll { (a: U, b: U, c: U) =>
        val rng = implicitly[Ring[T]]
        Equiv[T].equiv(rng.times(a, rng.plus(b, c)), rng.plus(rng.times(a, b), rng.times(a, c))) &&
        Equiv[T].equiv(rng.times(rng.plus(b, c), a), rng.plus(rng.times(b, a), rng.times(c, a)))
      }

  def isDistributive[T: Ring: Arbitrary: Equiv]: Prop =
    isDistributiveDifferentTypes[T, T]

  def timesIsAssociative[T: Ring: Arbitrary: Equiv]: Prop =
    "timesIsAssociative" |: forAll { (a: T, b: T, c: T) =>
      val rng = implicitly[Ring[T]]
      Equiv[T].equiv(rng.times(a, rng.times(b, c)), rng.times(rng.times(a, b), c))
    }

  def pseudoRingLaws[T: Ring: Arbitrary: Equiv]: Prop =
    isDistributive[T] && timesIsAssociative[T] && groupLaws[T] &&
      isCommutative[T] && isNonZeroWorksRing[T]

  def semiringLaws[T: Ring: Arbitrary: Equiv]: Prop =
    isDistributive[T] && timesIsAssociative[T] &&
      validOne[T] && commutativeMonoidLaws[T] &&
      zeroAnnihilates[T] && isNonZeroWorksRing[T]

  def ringLaws[T: Ring: Arbitrary: Equiv]: Prop =
    validOne[T] && pseudoRingLaws[T]
}
