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
import org.scalacheck.Prop
import org.scalacheck.Prop.forAll
import scala.math.Equiv

/**
 * Base properties useful for all tests using Algebird's typeclasses.
 */

object BaseProperties {
  def defaultEq[T](t0: T, t1: T) = t0 == t1

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

  def isAssociativeEq[T: Semigroup, U <: T: Arbitrary](eqfn: (T, T) => Boolean) = {
    'isAssociativeEq |: forAll { (a: U, b: U, c: U) =>
      val semi = implicitly[Semigroup[T]]
      eqfn(semi.plus(a, semi.plus(b, c)), semi.plus(semi.plus(a, b), c)) //&& false
    }
  }

  def isAssociativeDifferentTypes[T: Semigroup, U <: T: Arbitrary] =
    isAssociativeEq[T, U](defaultEq _)

  def isAssociative[T: Semigroup: Arbitrary] = isAssociativeDifferentTypes[T, T]

  def semigroupSumWorks[T: Semigroup: Arbitrary: Equiv] = 'semigroupSumWorks |: forAll { (head: T, tail: List[T]) =>
    Equiv[T].equiv(Semigroup.sumOption(head :: tail).get, tail.foldLeft(head)(Semigroup.plus(_, _)))
  }

  def isCommutativeEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean) = 'isCommutativeEq |: forAll { (a: T, b: T) =>
    val semi = implicitly[Semigroup[T]]
    eqfn(semi.plus(a, b), semi.plus(b, a))
  }
  def isCommutative[T: Semigroup: Arbitrary] = isCommutativeEq[T](defaultEq _)

  def semigroupLaws[T: Semigroup: Arbitrary] = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(defaultEq)
    semigroupLawsEquiv[T]
  }

  def semigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean) = {
    implicit val eq: Equiv[T] = Equiv.fromFunction(eqfn)
    semigroupLawsEquiv[T]
  }

  def semigroupLawsEquiv[T: Semigroup: Arbitrary: Equiv] =
    isAssociativeEq[T, T](Equiv[T].equiv _) && semigroupSumWorks[T]

  def commutativeSemigroupLawsEq[T: Semigroup: Arbitrary](eqfn: (T, T) => Boolean) =
    isAssociativeEq[T, T](eqfn) && isCommutativeEq[T](eqfn)
  def commutativeSemigroupLaws[T: Semigroup: Arbitrary] = commutativeSemigroupLawsEq[T](defaultEq _)

  def isNonZeroWorksMonoid[T: Monoid: Arbitrary: Equiv] = 'isNonZeroWorksMonoid |: forAll { (a: T, b: T) =>
    val aIsLikeZero = Monoid.zeroEquiv[T].equiv(Monoid.plus(a, b), b)
    Monoid.isNonZero(a) || aIsLikeZero
  }

  def isNonZeroWorksRing[T: Ring: Arbitrary] = 'isNonZeroWorksRing |: forAll { (a: T, b: T) =>
    implicit val monT: Monoid[T] = implicitly[Ring[T]]
    val prodZero = !monT.isNonZero(Ring.times(a, b))
    (Monoid.isNonZero(a) && Monoid.isNonZero(b)) || prodZero
  }

  def weakZeroDifferentTypes[T: Monoid, U <: T: Arbitrary] = 'weakZeroDifferentTypes |: forAll { (a: U) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    // Some types, e.g. Maps, are not totally equal for all inputs (i.e. zero values removed)
    (mon.plus(a, zero) == mon.plus(zero, a))
  }

  def weakZero[T: Monoid: Arbitrary] = weakZeroDifferentTypes[T, T]

  def validZeroEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean) = 'validZeroEq |: forAll { (a: T) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    eqfn(a, mon.plus(a, zero)) && eqfn(mon.plus(zero, a), a) && eqfn(mon.plus(a, zero), mon.plus(zero, a))
  }
  def validZero[T: Monoid: Arbitrary] = validZeroEq[T](defaultEq _)

  def monoidLaws[T: Monoid: Arbitrary] = validZero[T] && semigroupLaws[T] && isNonZeroWorksMonoid[T]

  def monoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean): Prop =
    validZeroEq[T](eqfn) && semigroupLawsEq[T](eqfn)

  def monoidLawsEquiv[T: Monoid: Arbitrary: Equiv]: Prop =
    monoidLawsEq[T](Equiv[T].equiv)

  def commutativeMonoidLawsEq[T: Monoid: Arbitrary](eqfn: (T, T) => Boolean) =
    monoidLawsEq[T](eqfn) && isCommutativeEq[T](eqfn)

  def commutativeMonoidLaws[T: Monoid: Arbitrary] = commutativeMonoidLawsEq[T](defaultEq _)

  def hasAdditiveInversesDifferentTypes[T: Group, U <: T: Arbitrary] = forAll { (a: U) =>
    val grp = implicitly[Group[T]]
    (!grp.isNonZero(grp.plus(grp.negate(a), a))) &&
      (!grp.isNonZero(grp.minus(a, a))) &&
      (!grp.isNonZero(grp.plus(a, grp.negate(a))))
  }

  def hasAdditiveInverses[T: Group: Arbitrary] = hasAdditiveInversesDifferentTypes[T, T]

  def groupLawsEq[T: Group: Arbitrary](eqfn: (T, T) => Boolean) = monoidLawsEq[T](eqfn) && hasAdditiveInverses[T]

  def groupLaws[T: Group: Arbitrary] = monoidLaws[T] && hasAdditiveInverses[T]
  // Here are multiplicative properties:
  def validOne[T: Ring: Arbitrary] = 'validOne |: forAll { (a: T) =>
    val rng = implicitly[Ring[T]]
    (rng.times(rng.one, a) == rng.times(a, rng.one)) && (a == rng.times(a, rng.one))
  }
  def zeroAnnihilates[T: Ring: Arbitrary] = 'zeroAnnihilates |: forAll { (a: T) =>
    val ring = implicitly[Ring[T]]
    (!ring.isNonZero(ring.times(a, ring.zero))) &&
      (!ring.isNonZero(ring.times(ring.zero, a)))
  }
  def isDistributiveDifferentTypes[T: Ring, U <: T: Arbitrary] = 'isDistributiveDifferentTypes |:
    forAll { (a: U, b: U, c: U) =>
      val rng = implicitly[Ring[T]]
      (rng.times(a, rng.plus(b, c)) == rng.plus(rng.times(a, b), rng.times(a, c))) &&
        (rng.times(rng.plus(b, c), a) == rng.plus(rng.times(b, a), rng.times(c, a)))
    }
  def isDistributive[T: Ring: Arbitrary] = isDistributiveDifferentTypes[T, T]

  def timesIsAssociative[T: Ring: Arbitrary] = 'timesIsAssociative |: forAll { (a: T, b: T, c: T) =>
    val rng = implicitly[Ring[T]]
    rng.times(a, rng.times(b, c)) == rng.times(rng.times(a, b), c)
  }
  def pseudoRingLaws[T: Ring: Arbitrary] =
    isDistributive[T] && timesIsAssociative[T] && groupLaws[T] && isCommutative[T] &&
      isNonZeroWorksRing[T]

  def semiringLaws[T: Ring: Arbitrary] =
    isDistributive[T] && timesIsAssociative[T] &&
      validOne[T] && commutativeMonoidLaws[T] &&
      zeroAnnihilates[T] &&
      isNonZeroWorksRing[T]

  def ringLaws[T: Ring: Arbitrary] = validOne[T] && pseudoRingLaws[T]

  def hasMultiplicativeInverse[T: Field: Arbitrary] = 'hasMultiplicativeInverse |: forAll { (a: T) =>
    val fld = implicitly[Field[T]]
    (!fld.isNonZero(a)) || {
      val inva = fld.inverse(a)
      (fld.times(inva, a) == fld.one) && (fld.times(a, inva) == fld.one)
    }
  }
  def fieldLaws[T: Field: Arbitrary] = ringLaws[T] && hasMultiplicativeInverse[T]
}
