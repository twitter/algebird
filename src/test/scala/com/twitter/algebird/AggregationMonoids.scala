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
    cnt <- choose(Int.MinValue.toLong, Int.MaxValue.toLong)
    v <- choose(-1e100, 1e100) // Don't get too big and overflow
  } yield AveragedValue(cnt, v) }

  property("AveragedValue Group laws") = groupLawsEq[AveragedValue] { (avl, avr) =>
    ((avl.count == 0L) && (avr.count == 0L)) || {
      approxEq(avl.value, avr.value) && (avl.count == avr.count)
    }
  }

  implicit val momentsgen = Arbitrary { for {
    m0 <- choose(1L,Int.MaxValue.toLong)
    m1 <- choose(-1e50, 1e50)
    m2 <- choose(0, 1e50)
    m3 <- choose(-1e10, 1e50)
    m4 <- choose(0, 1e50)
  } yield new Moments(m0, m1, m2, m3, m4) }

  property("Moments Group laws") = groupLawsEq[Moments] { (ml, mr) =>
    (ml.m0 == mr.m0) &&
    approxEq(ml.m1, mr.m1) &&
    approxEq(ml.m2, mr.m2) &&
    approxEq(ml.m3, mr.m3) &&
    approxEq(ml.m4, mr.m4)
  }
}
