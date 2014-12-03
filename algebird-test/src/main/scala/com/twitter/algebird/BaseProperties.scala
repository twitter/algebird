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

import org.scalacheck.Arbitrary
import org.scalatest.prop.{ PropertyChecks, Checkers }
import org.scalatest.Matchers
import org.scalacheck.Prop
import org.scalactic.Equality
import scala.math.Equiv

/**
 * Base properties useful for all tests using Algebird's typeclasses.
 */

object BaseProperties {
  import Checkers._
  import Matchers._
  import PropertyChecks._
  def defaultEq[T](t0: T, t1: T) = t0 == t1

  private def eqFnToEquality[T](eqfn: (T, T) => Boolean): Equality[T] =
    new Equality[T] {
      def areEqual(a: T, b: Any): Boolean = eqfn(b.asInstanceOf[T], a)
    }

  private def equivToEquality[T: Equiv]: Equality[T] =
    new Equality[T] {
      def areEqual(a: T, b: Any): Boolean = implicitly[Equiv[T]].equiv(b.asInstanceOf[T], a)
    }

  trait HigherEq[M[_]] {
    def apply[T](m: M[T], n: M[T]): Boolean
  }

  class DefaultHigherEq[M[_]] extends HigherEq[M] {
    override def apply[T](m: M[T], n: M[T]) = m == n
  }

  def isNonZero[V: Semigroup](v: V) = implicitly[Semigroup[V]] match {
    case mon: Monoid[_] => mon.isNonZero(v)
    case _ => true
  }

  def isAssociativeEquality[T: Semigroup: Equality, U <: T: Arbitrary] =
    forAll { (a: U, b: U, c: U) =>
      val semi = implicitly[Semigroup[T]]
      semi.plus(a, semi.plus(b, c)) shouldEqual semi.plus(semi.plus(a, b), c)
    }

  def isAssociativeDifferentTypes[T: Semigroup, U <: T: Arbitrary] =
    isAssociativeEquality[T, U]

  def isAssociative[T: Semigroup: Arbitrary] =
    isAssociativeDifferentTypes[T, T]

  def semigroupSumWorksEquality[T: Semigroup: Arbitrary: Equality] = {
    forAll { (in: List[T]) =>
      whenever(!in.isEmpty) {
        Semigroup.sumOption(in).get shouldEqual in.reduceLeft(Semigroup.plus(_, _))
      }
    }
  }

  def semigroupSumWorks[T: Semigroup: Arbitrary: Equiv] = {
    implicit val eq = equivToEquality[T]
    semigroupSumWorksEquality
  }

  def isCommutativeEquality[T: Semigroup: Arbitrary: Equality] = forAll { (a: T, b: T) =>
    val semi = implicitly[Semigroup[T]]
    semi.plus(a, b) shouldEqual semi.plus(b, a)
  }

  def isCommutative[T: Semigroup: Arbitrary] = isCommutativeEquality[T]

  def semigroupLaws[T: Semigroup: Arbitrary] =
    semigroupLawsEquality[T]

  def semigroupLawsEquiv[T: Semigroup: Arbitrary: Equiv] = {
    implicit val eq = equivToEquality[T]
    semigroupLawsEquality
  }

  def semigroupLawsEquality[T: Semigroup: Arbitrary: Equality] = {
    isAssociativeEquality[T, T]
    semigroupSumWorksEquality[T]
  }

  def commutativeSemigroupLawsEquality[T: Semigroup: Arbitrary: Equality] = {
    isAssociativeEquality[T, T]
    isCommutativeEquality[T]
  }

  def commutativeSemigroupLaws[T: Semigroup: Arbitrary] = commutativeSemigroupLawsEquality[T]

  def isNonZeroWorksMonoid[T: Monoid: Arbitrary: Equiv] = Checkers.check(Prop.forAll { (a: T, b: T) =>
    val aIsLikeZero = Monoid.zeroEquiv[T].equiv(Monoid.plus(a, b), b)
    Monoid.isNonZero(a) || aIsLikeZero
  })

  def isNonZeroWorksRing[T: Ring: Arbitrary] = Checkers.check(Prop.forAll { (a: T, b: T) =>
    implicit val monT: Monoid[T] = implicitly[Ring[T]]
    val prodZero = !monT.isNonZero(Ring.times(a, b))
    (Monoid.isNonZero(a) && Monoid.isNonZero(b)) || prodZero
  })

  def weakZeroDifferentTypes[T: Monoid, U <: T: Arbitrary] = forAll { (a: U) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    // Some types, e.g. Maps, are not totally equal for all inputs (i.e. zero values removed)
    mon.plus(a, zero) shouldEqual mon.plus(zero, a)
  }

  def weakZero[T: Monoid: Arbitrary] = weakZeroDifferentTypes[T, T]

  def validZeroEquality[T: Monoid: Arbitrary: Equality] = forAll { (a: T) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    a shouldEqual mon.plus(a, zero)
    mon.plus(zero, a) shouldEqual a
    mon.plus(a, zero) shouldEqual mon.plus(zero, a)
  }

  def validZero[T: Monoid: Arbitrary] = validZeroEquality[T]

  def monoidLaws[T: Monoid: Arbitrary] = {
    validZero[T]
    semigroupLaws[T]
    isNonZeroWorksMonoid[T]
  }

  def monoidLawsEquality[T: Monoid: Arbitrary: Equality] = {
    validZeroEquality[T]
    semigroupLawsEquality[T]
  }

  def commutativeMonoidLawsEquality[T: Monoid: Arbitrary: Equality] = {
    monoidLawsEquality[T]
    isCommutativeEquality[T]
  }

  def commutativeMonoidLaws[T: Monoid: Arbitrary] = commutativeMonoidLawsEquality[T]

  def hasAdditiveInversesDifferentTypes[T: Group, U <: T: Arbitrary] = forAll { (a: U) =>
    val grp = implicitly[Group[T]]
    assert(grp.isNonZero(grp.plus(grp.negate(a), a)) == false)
    assert(grp.isNonZero(grp.minus(a, a)) == false)
    assert(grp.isNonZero(grp.plus(a, grp.negate(a))) == false)
  }

  def hasAdditiveInverses[T: Group: Arbitrary] =
    hasAdditiveInversesDifferentTypes[T, T]

  def groupLawsEquality[T: Group: Arbitrary: Equality] = {
    monoidLawsEquality[T]
    hasAdditiveInverses[T]
  }

  def groupLaws[T: Group: Arbitrary] = {
    monoidLaws[T]
    hasAdditiveInverses[T]
  }

  // Here are multiplicative properties:
  def validOne[T: Ring: Arbitrary] = Checkers.check(Prop.forAll { (a: T) =>
    val rng = implicitly[Ring[T]]
    (rng.times(rng.one, a) == rng.times(a, rng.one)) && (a == rng.times(a, rng.one))
  })

  def zeroAnnihilates[T: Ring: Arbitrary] = Checkers.check(Prop.forAll { (a: T) =>
    val ring = implicitly[Ring[T]]
    (!ring.isNonZero(ring.times(a, ring.zero))) &&
      (!ring.isNonZero(ring.times(ring.zero, a)))
  })

  def isDistributiveDifferentTypes[T: Ring, U <: T: Arbitrary] = Checkers.check(Prop.forAll { (a: U, b: U, c: U) =>
    val rng = implicitly[Ring[T]]
    (rng.times(a, rng.plus(b, c)) == rng.plus(rng.times(a, b), rng.times(a, c))) &&
      (rng.times(rng.plus(b, c), a) == rng.plus(rng.times(b, a), rng.times(c, a)))
  })

  def isDistributive[T: Ring: Arbitrary] = isDistributiveDifferentTypes[T, T]

  def timesIsAssociative[T: Ring: Arbitrary] = Checkers.check(Prop.forAll { (a: T, b: T, c: T) =>
    val rng = implicitly[Ring[T]]
    rng.times(a, rng.times(b, c)) == rng.times(rng.times(a, b), c)
  })

  def pseudoRingLaws[T: Ring: Arbitrary] = {
    isDistributive[T]
    timesIsAssociative[T]
    groupLaws[T]
    isCommutative[T]
    isNonZeroWorksRing[T]
  }

  def semiringLaws[T: Ring: Arbitrary] = {
    isDistributive[T]
    timesIsAssociative[T]
    validOne[T]
    commutativeMonoidLaws[T]
    zeroAnnihilates[T]
    isNonZeroWorksRing[T]
  }

  def ringLaws[T: Ring: Arbitrary] = {
    validOne[T]
    pseudoRingLaws[T]
  }

  def hasMultiplicativeInverse[T: Field: Arbitrary] = Checkers.check(Prop.forAll { (a: T) =>
    val fld = implicitly[Field[T]]
    (!fld.isNonZero(a)) || {
      val inva = fld.inverse(a)
      (fld.times(inva, a) == fld.one) && (fld.times(a, inva) == fld.one)
    }
  })

  def fieldLaws[T: Field: Arbitrary] = {
    ringLaws[T]
    hasMultiplicativeInverse[T]
  }

  // Deprecated methods supplying equality as an a == b method
  @deprecated("Use isAssociativeEquality instead.", "2014-11-30")
  def isAssociativeEq[T: Semigroup, U <: T: Arbitrary](eqfn: (T, T) => Boolean) {
    implicit val eq = eqFnToEquality(eqfn)
    isAssociativeEquality[T, U]
  }

  @deprecated("Use isCommutativeEquality instead.", "2014-11-30")
  def isCommutativeEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq = eqFnToEquality(eqfn)
    isCommutativeEquality[T]
  }

  @deprecated("Use validZeroEquality instead.", "2014-11-30")
  def validZeroEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq = eqFnToEquality(eqfn)
    validZeroEquality[T]
  }

  @deprecated("Use semigroupLawsEquality instead.", "2014-11-30")
  def semigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq = eqFnToEquality(eqfn)
    semigroupLawsEquality[T]
  }

  @deprecated("Use commutativeSemigroupLawsEquality instead.", "2014-11-30")
  def commutativeSemigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq = eqFnToEquality(eqfn)
    commutativeSemigroupLawsEquality[T]
  }

  @deprecated("Use commutativeMonoidLawsEquality instead.", "2014-11-30")
  def commutativeMonoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq = eqFnToEquality(eqfn)
    commutativeMonoidLawsEquality[T]
  }

  @deprecated("Use groupLawsEquality instead.", "2014-11-30")
  def groupLawsEq[T: Group: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq = eqFnToEquality(eqfn)
    groupLawsEquality[T]
  }

  @deprecated("Use monoidLawsEquality instead.", "2014-11-30")
  def monoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq = eqFnToEquality(eqfn)
    monoidLawsEquality[T]
  }
}
