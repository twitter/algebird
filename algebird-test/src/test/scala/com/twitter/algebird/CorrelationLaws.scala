package com.twitter.algebird

import org.scalatest._
import org.scalacheck.Prop.forAll
import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._



class CorrelationLaws extends CheckProperties {
  val EPS = 1e-10

  property("Correlation group laws") {
    implicit val equiv: Equiv[Correlation] =
      Equiv.fromFunction { (corr1, corr2) =>
        approxEq(EPS)(corr1.c2, corr2.c2) &&
          approxEq(EPS)(corr1.m2Left, corr2.m2Left) &&
          approxEq(EPS)(corr1.m2Right, corr2.m2Right) &&
          approxEq(EPS)(corr1.m1Left, corr2.m1Left) &&
          approxEq(EPS)(corr1.m1Right, corr2.m1Right) &&
          (corr1.m0 == corr2.m0)
      }
    groupLaws[Correlation]
  }

  property("Central moments 0 through 2 match implementation from Moments") {
    forAll{ (corr1: Correlation, corr2: Correlation) =>
      val corr = Monoid.plus(corr1, corr2)
      val leftMoment = Monoid.plus(corr1.leftMoment, corr2.leftMoment)
      val rightMoment = Monoid.plus(corr1.rightMoment, corr2.rightMoment)

      println("="*80)
      println(corr.leftMoment)
      println(leftMoment)
      println(corr.rightMoment)
      println(rightMoment)
      println("*"*80)
      corr.count == leftMoment.count  &&
      corr.count == rightMoment.count &&
      approxEq(EPS)(corr.meanLeft, leftMoment.mean) &&
      approxEq(EPS)(corr.meanRight, rightMoment.mean) &&
      approxEq(EPS)(corr.stddevLeft, leftMoment.stddev) &&
      approxEq(EPS)(corr.stddevRight, rightMoment.stddev)

    }
  }
}

class CorrelationTest extends WordSpec with Matchers {
  def testApproxEq(f1: Double, f2: Double) {
    if (f2 == 0)
      assert(f1 < 1e-10)
    else
      assert((scala.math.abs(f1 - f2) / scala.math.abs(f2)) < 1e-10)
  }

  val aggregator = CorrelationAggregator.andThenPresent(_.correlation)



}