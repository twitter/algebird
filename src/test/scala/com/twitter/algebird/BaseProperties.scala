package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

trait BaseProperties {
  def defaultEq[T](t0 : T, t1 : T) = t0 == t1

  def weakZero[T : Monoid : Arbitrary] = forAll { (a : T) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    // Some types, e.g. Maps, are not totally equal for all inputs (i.e. zero values removed)
    (mon.plus(a, zero) == mon.plus(zero, a))
  }

  def validZeroEq[T : Monoid : Arbitrary](eqfn : (T,T) => Boolean) = forAll { (a : T) =>
    val mon = implicitly[Monoid[T]]
    val zero = mon.zero
    eqfn(a, mon.plus(a, zero)) && eqfn(mon.plus(zero, a), a) && eqfn(mon.plus(a,zero),mon.plus(zero,a))
  }
  def validZero[T : Monoid : Arbitrary] = validZeroEq[T](defaultEq _)

  def isAssociativeEq[T : Monoid : Arbitrary](eqfn : (T,T) => Boolean) = forAll { (a : T, b : T, c : T) =>
    val mon = implicitly[Monoid[T]]
    eqfn(mon.plus(a, mon.plus(b,c)), mon.plus(mon.plus(a,b), c))
  }
  def isAssociative[T : Monoid : Arbitrary] = isAssociativeEq[T](defaultEq _)

  def monoidLaws[T : Monoid : Arbitrary] = validZero[T] && isAssociative[T]
  def monoidLawsEq[T : Monoid : Arbitrary](eqfn : (T,T) => Boolean) = {
    validZeroEq[T](eqfn) && isAssociativeEq[T](eqfn)
  }

  def hasAdditiveInverses[T: Group : Arbitrary] = forAll { (a : T) =>
    val grp = implicitly[Group[T]]
    (!grp.isNonZero(grp.plus(grp.negate(a), a))) &&
      (!grp.isNonZero(grp.minus(a,a))) &&
      (!grp.isNonZero(grp.plus(a, grp.negate(a))))
  }

  def groupLawsEq[T : Group : Arbitrary](eqfn : (T,T) => Boolean) = monoidLawsEq[T](eqfn) && hasAdditiveInverses[T]

  def groupLaws[T : Group : Arbitrary] = monoidLaws[T] && hasAdditiveInverses[T]

  def validOne[T : Ring : Arbitrary] = forAll { (a : T) =>
    val rng = implicitly[Ring[T]]
    (rng.times(rng.one, a) == rng.times(a, rng.one)) && (a == rng.times(a, rng.one))
  }
  def isDistributive[T : Ring : Arbitrary] = forAll { (a : T, b : T, c : T) =>
    val rng = implicitly[Ring[T]]
    (rng.times(a, rng.plus(b,c)) == rng.plus(rng.times(a,b), rng.times(a,c))) &&
      (rng.times(rng.plus(b,c),a) == rng.plus(rng.times(b,a), rng.times(c,a)))
  }
  def timesIsAssociative[T : Ring : Arbitrary] = forAll { (a : T, b : T, c : T) =>
    val rng = implicitly[Ring[T]]
    rng.times(a, rng.times(b,c)) == rng.times(rng.times(a,b),c)
  }
  def pseudoRingLaws[T:Ring:Arbitrary] = isDistributive[T] && timesIsAssociative[T] && groupLaws[T]
  def semiringLaws[T:Ring:Arbitrary] =
    isDistributive[T] && timesIsAssociative[T] && validOne[T] && validZero[T]
  def ringLaws[T : Ring : Arbitrary] = validOne[T] && pseudoRingLaws[T]

  def hasMultiplicativeInverse[T : Field : Arbitrary] = forAll { (a : T) =>
    val fld = implicitly[Field[T]]
    (!fld.isNonZero(a)) || {
      val inva = fld.inverse(a)
      (fld.times(inva, a) == fld.one) && (fld.times(a, inva) == fld.one)
    }
  }
  def fieldLaws[T : Field : Arbitrary] = ringLaws[T] && hasMultiplicativeInverse[T]
}
