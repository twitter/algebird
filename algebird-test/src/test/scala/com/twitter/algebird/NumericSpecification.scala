package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Prop

/**
 * Tests abstract algebra against scala's Numeric trait
 * Numeric is basically the ring trait with ordering, so we can use it
 * below to test all the numeric traits.
 */
class NumericSpecification extends PropSpec with PropertyChecks with Matchers {
  def plusNumericProp[T: HasAdditionOperatorAndZero: Numeric: Arbitrary] = Prop.forAll { (a: T, b: T) =>
    val mon = implicitly[HasAdditionOperatorAndZero[T]]
    val num = implicitly[Numeric[T]]
    num.plus(a, b) == mon.plus(a, b)
  }
  property("Int plus") {
    plusNumericProp[Int]
  }

  property("Long plus") {
    plusNumericProp[Long]
  }

  property("Double plus") {
    plusNumericProp[Double]
  }

  property("Float plus") {
    plusNumericProp[Float]
  }

  def zeroNumericProp[T: HasAdditionOperatorAndZero: Group: Numeric: Arbitrary] = Prop.forAll { (a: T) =>
    val mon = implicitly[HasAdditionOperatorAndZero[T]]
    val grp = implicitly[Group[T]]
    val num = implicitly[Numeric[T]]
    (a == mon.plus(mon.zero, a)) &&
      (a == mon.plus(a, mon.zero)) &&
      (a == grp.minus(a, grp.zero)) &&
      (mon.nonZeroOption(a) == Some(a).filter { _ != num.zero })
  }

  def zeroProps[T: HasAdditionOperatorAndZero: Numeric] = {
    val mon = implicitly[HasAdditionOperatorAndZero[T]]
    val num = implicitly[Numeric[T]]
    (num.zero == mon.zero) && (!mon.isNonZero(mon.zero)) && (mon.nonZeroOption(mon.zero) == None)
  }

  property("Int zero") {
    zeroNumericProp[Int] && zeroProps[Int]
  }

  property("Long zero") {
    zeroNumericProp[Long] && zeroProps[Long]
  }

  property("Double zero") {
    zeroNumericProp[Double] && zeroProps[Double]
  }

  property("Float zero") {
    zeroNumericProp[Float] && zeroProps[Float]
  }

  def minusNumericProp[T: Group: Numeric: Arbitrary] = Prop.forAll { (a: T, b: T) =>
    val grp = implicitly[Group[T]]
    val num = implicitly[Numeric[T]]
    num.minus(a, b) == grp.minus(a, b)
  }

  property("Int minus") {
    minusNumericProp[Int]
  }

  property("Long minus") {
    minusNumericProp[Long]
  }

  property("Double minus") {
    minusNumericProp[Double]
  }

  property("Float minus") {
    minusNumericProp[Float]
  }

  def oneNumericProp[T: Ring: Numeric: Arbitrary] = Prop.forAll { (a: T) =>
    val ring = implicitly[Ring[T]]
    val num = implicitly[Numeric[T]]
    (num.one == ring.one) && (a == ring.times(a, ring.one)) && (a == ring.times(ring.one, a))
  }
  property("Int one") {
    oneNumericProp[Int]
  }

  property("Long one") {
    oneNumericProp[Long]
  }

  property("Double one") {
    oneNumericProp[Double]
  }

  property("Float one") {
    oneNumericProp[Float]
  }

  def timesNumericProp[T: Ring: Numeric: Arbitrary] = Prop.forAll { (a: T, b: T) =>
    val ring = implicitly[Ring[T]]
    val num = implicitly[Numeric[T]]
    num.times(a, b) == ring.times(a, b)
  }

  property("Int times") {
    timesNumericProp[Int]
  }

  property("Long times") {
    timesNumericProp[Long]
  }

  property("Double times") {
    timesNumericProp[Double]
  }

  property("Float times") {
    timesNumericProp[Float]
  }

  property("Double div") {
    forAll { (a: Double, b: Double) =>
      val fld = implicitly[Field[Double]]
      assert((!fld.isNonZero(b)) || ((a / b) == fld.div(a, b)))
    }
  }

  property("Float div") {
    forAll { (a: Float, b: Float) =>
      val fld = implicitly[Field[Float]]
      assert((!fld.isNonZero(b)) || ((a / b) == fld.div(a, b)))
    }
  }
}
