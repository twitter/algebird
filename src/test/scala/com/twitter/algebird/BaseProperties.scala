package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

trait BaseProperties {
  def weakZero[T : Monoid : Arbitrary] = forAll { (a : T) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    // Some types, e.g. Maps, are not totally equal for all inputs (i.e. zero values removed)
    (mon.plus(a, zero) == mon.plus(zero, a))
  }

  def validZero[T : Monoid : Arbitrary] = forAll { (a : T) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
   (a == mon.plus(a, zero))
  } && weakZero[T]

  def validZeroEq[T : Monoid : Arbitrary](eqfn : (T,T) => Boolean) = forAll { (a : T) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    eqfn(a, mon.plus(a, zero)) && eqfn(mon.plus(zero, a), a) && eqfn(mon.plus(a,zero),mon.plus(zero,a))
  }

  def isAssociative[T : Monoid : Arbitrary] = forAll { (a : T, b : T, c : T) =>
    val mon = implicitly[Monoid[T]]
    mon.plus(a, mon.plus(b,c)) == mon.plus(mon.plus(a,b), c)
  }
  def isAssociativeEq[T : Monoid : Arbitrary](eqfn : (T,T) => Boolean) = forAll { (a : T, b : T, c : T) =>
    val mon = implicitly[Monoid[T]]
    eqfn(mon.plus(a, mon.plus(b,c)), mon.plus(mon.plus(a,b), c))
  }
  def monoidLaws[T : Monoid : Arbitrary] = validZero[T] && isAssociative[T]
  def monoidLawsEq[T : Monoid : Arbitrary](eqfn : (T,T) => Boolean) = {
    validZeroEq[T](eqfn) && isAssociativeEq[T](eqfn)
  }

  def hasAdditiveInverses[T: Group : Arbitrary] = forAll { (a : T) =>
    val grp = implicitly[Group[T]]
    grp.plus(grp.negate(a), a) == grp.zero
  }

  def groupLaws[T : Group : Arbitrary] = monoidLaws[T] && hasAdditiveInverses[T]

  def validOne[T : Ring : Arbitrary] = forAll { (a : T) =>
    val rng = implicitly[Ring[T]]
    (rng.times(rng.one, a) == rng.times(a, rng.one)) && (a == rng.times(a, rng.one))
  }
  def isDistributive[T : Ring : Arbitrary] = forAll { (a : T, b : T, c : T) =>
    val rng = implicitly[Ring[T]]
    rng.times(a, rng.plus(b,c)) == rng.plus(rng.times(a,b), rng.times(a,c))
  }
  def ringLaws[T : Ring : Arbitrary] = validOne[T] && isDistributive[T] && groupLaws[T]
}

