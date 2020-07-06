package com.twitter.algebird

import org.scalacheck.Prop.forAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._

object CorrelationLaws {
  val EPS = 1e-10

  def aggregateFunction(f: Double => Double): Aggregator[Double, Correlation, Double] =
    CorrelationAggregator
      .composePrepare[Double](x => (x, f(x)))
      .andThenPresent(_.correlation)

  val testList = Range.inclusive(-10, 10).map(_.toDouble).toList

  def corrApproxEq(corr1: Correlation, corr2: Correlation): Boolean =
    approxEqOrBothNaN(EPS)(corr1.c2, corr2.c2) &&
      approxEqOrBothNaN(EPS)(corr1.m2Left, corr2.m2Left) &&
      approxEqOrBothNaN(EPS)(corr1.m2Right, corr2.m2Right) &&
      approxEqOrBothNaN(EPS)(corr1.m1Left, corr2.m1Left) &&
      approxEqOrBothNaN(EPS)(corr1.m1Right, corr2.m1Right) &&
      approxEqOrBothNaN(EPS)(corr1.m0, corr2.m0)

}

class CorrelationLaws extends CheckProperties {

  import CorrelationLaws._

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
    forAll { (l: List[(Double, Double)]) =>
      val corr = Monoid.sum(l.map(Correlation.apply))
      val leftMoment = Monoid.sum(l.map { case (x, _) => Moments.apply(x) })
      val rightMoment = Monoid.sum(l.map { case (_, y) => Moments.apply(y) })
      approxEq(EPS)(corr.totalWeight, leftMoment.count) &&
      approxEq(EPS)(corr.totalWeight, rightMoment.count) &&
      approxEq(EPS)(corr.meanLeft, leftMoment.mean) &&
      approxEq(EPS)(corr.meanRight, rightMoment.mean) &&
      (l.length < 2 ||
      (approxEqOrBothNaN(EPS)(corr.stddevLeft, leftMoment.stddev) &&
      approxEqOrBothNaN(EPS)(corr.stddevRight, rightMoment.stddev)))
    }
  }

  property("central moments of a line return the slope of the line") {
    // using arbitrary floating point values for this test yields far less numerical precision
    forAll { (m: Int, b: Int) =>
      val calculatedCorrelation = aggregateFunction(x => m * x + b)(testList)
      (m == 0.0
      || (m > 0.0 && approxEq(EPS)(calculatedCorrelation, 1.0))
      || (m < 0.0 && approxEq(EPS)(calculatedCorrelation, -1.0)))
    }
  }

  property("given points exactly on a straight line, least squares approximation finds slope and intercept") {
    // using arbitrary floating point values for this test yields far less numerical precision
    forAll { (m: Int, b: Int) =>
      val (mOut, bOut) =
        CorrelationAggregator
          .composePrepare[Double](x => (x, m * x + b))
          .andThenPresent(_.linearLeastSquares)(testList)

      approxEq(EPS)(m.toDouble, mOut) && approxEq(EPS)(b.toDouble, bOut)
    }
  }

  property("the swap method on moments works as you'd think") {
    forAll { l: List[(Double, Double)] =>
      val swapped = CorrelationAggregator(l).swap
      val reversedInput = CorrelationAggregator.composePrepare[(Double, Double)] { case (x, y) => (y, x) }(l)
      corrApproxEq(swapped, reversedInput)
    }
  }
}

class CorrelationTest extends AnyWordSpec with Matchers {
  import CorrelationLaws._

  "correlation with y = x*x should be 0" in {
    aggregateFunction(x => x * x)(testList) should equal(0.0)
  }

}
