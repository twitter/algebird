package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import com.twitter.algebird.scalacheck.NonEmptyVector
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop

class AveragedValueLaws extends CheckProperties {
  def avg[T](v: Vector[T])(implicit num: Numeric[T]): Double = {
    val items = v.map(num.toDouble(_))
    val sum = items.sum
    val count = items.size
    sum / count
  }

  property("AveragedValue is a group and commutative") {
    implicit val equiv: Equiv[AveragedValue] =
      Equiv.fromFunction { (avl, avr) =>
        avl.count == 0L && avr.count == 0L || {
          approxEq(1e-10)(avl.value, avr.value) && avl.count == avr.count
        }
      }
    groupLaws[AveragedValue] && isCommutative[AveragedValue]
  }

  property("AveragedValue.aggregator returns the average") {
    forAll { v: NonEmptyVector[Double] => approxEq(1e-10)(avg(v.items), AveragedValue.aggregator(v.items)) }
  }

  property("AveragedValue instances subtract") {
    forAll { (l: AveragedValue, r: AveragedValue) =>
      l + -l == Monoid.zero[AveragedValue] &&
      l - l == Monoid.zero[AveragedValue] &&
      l - r == l + -r
    }
  }

  property("AveragedValue can absorb numbers directly") {
    forAll((base: AveragedValue, x: BigInt) => base + AveragedValue(x) == base + x)
  }

  property("AveragedValue works by + or sumOption") {
    forAll { v: NonEmptyVector[Double] =>
      val avgs = v.items.map(AveragedValue(_))
      val sumOpt = Semigroup.sumOption[AveragedValue](avgs).get
      val byPlus = avgs.reduce(_ + _)

      approxEq(1e-10)(avg(v.items), sumOpt.value) &&
      approxEq(1e-10)(sumOpt.value, byPlus.value)
    }
  }

  def numericAggregatorTest[T: Numeric: Arbitrary]: Prop =
    forAll { v: NonEmptyVector[T] =>
      val averaged = AveragedValue.numericAggregator[T].apply(v.items)
      approxEq(1e-10)(avg(v.items), averaged)
    }

  property("AveragedValue.numericAggregator averages BigInt") {
    numericAggregatorTest[BigInt]
  }

  property("AveragedValue.numericAggregator averages Long") {
    numericAggregatorTest[Long]
  }
}
