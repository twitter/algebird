package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

object AggregationMonoidSpecification extends Properties("Aggregations") with BaseProperties {

  def approxEq(f1 : Double, f2 : Double) = (scala.math.abs(f1 - f2) / scala.math.abs(f2)) < 1e-10

  implicit val dcgen = Arbitrary { for {
    a <- choose(-1e100, 1e100) // Don't get too big and overflow
    b <- choose(-1e100, 1e100) // Don't get too big and overflow
  } yield DecayedValue(a,b) }

  implicit val decayedMonoid = DecayedValue.monoidWithEpsilon(0.001)
  property("DecayedValue Monoid laws") = monoidLawsEq[DecayedValue] { (dvl, dvr) =>
    approxEq(dvl.value, dvr.value) && (dvl.scaledTime == dvr.scaledTime)
  }

  implicit val avgen = Arbitrary { for {
    cnt <- choose(1L,Int.MaxValue.toLong)
    v <- choose(-1e100, 1e100) // Don't get too big and overflow
  } yield AveragedValue(cnt, v) }

  property("AveragedValue Monoid laws") = monoidLawsEq[AveragedValue] { (avl, avr) =>
    approxEq(avl.value, avr.value) && (avl.count == avr.count)
  }
}
