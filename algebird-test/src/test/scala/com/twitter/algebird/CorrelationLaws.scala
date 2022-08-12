package com.twitter.algebird

import org.scalacheck.Prop.forAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._

object CorrelationLaws {
  val EPS: Double = 1e-10

  def aggregateFunction(f: Double => Double): Aggregator[Double, Correlation, Double] =
    CorrelationAggregator.correlation
      .composePrepare[Double](x => (x, f(x)))

  val testList: List[Double] = Range.inclusive(-10, 10).map(_.toDouble).toList

  def corrApproxEq(corr1: Correlation, corr2: Correlation): Boolean =
    approxEqOrBothNaN(EPS)(corr1.c2, corr2.c2) &&
      approxEqOrBothNaN(EPS)(corr1.m2x, corr2.m2x) &&
      approxEqOrBothNaN(EPS)(corr1.m2y, corr2.m2y) &&
      approxEqOrBothNaN(EPS)(corr1.m1x, corr2.m1x) &&
      approxEqOrBothNaN(EPS)(corr1.m1y, corr2.m1y) &&
      approxEqOrBothNaN(EPS)(corr1.m0, corr2.m0)

}

class CorrelationLaws extends CheckProperties {

  import CorrelationLaws._

  property("Correlation monoid laws") {
    implicit val equiv: Equiv[Correlation] =
      Equiv.fromFunction(corrApproxEq)
    monoidLaws[Correlation]
  }

  property("Central moments 0 through 2 match implementation from Moments") {
    forAll { (l: List[(Double, Double)]) =>
      val corr = Monoid.sum(l.map(Correlation.apply))
      val momentX = Monoid.sum(l.map { case (x, _) => Moments.apply(x) })
      val momentY = Monoid.sum(l.map { case (_, y) => Moments.apply(y) })
      approxEq(EPS)(corr.totalWeight, momentX.count) &&
      approxEq(EPS)(corr.totalWeight, momentY.count) &&
      approxEq(EPS)(corr.meanX, momentX.mean) &&
      approxEq(EPS)(corr.meanY, momentY.mean) &&
      (l.length < 2 ||
        (approxEqOrBothNaN(EPS)(corr.stddevX, momentX.stddev) &&
          approxEqOrBothNaN(EPS)(corr.stddevY, momentY.stddev)))
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
      val fn: ((Double, Double)) => (Double, Double) = { tup => tup.swap }

      val reversedInput = CorrelationAggregator.composePrepare[(Double, Double)](fn)(l)
      corrApproxEq(swapped, reversedInput)
    }
  }

  property("scaling by 0 and 1 works as you'd expect") {
    forAll { (corr: Correlation) =>
      corrApproxEq(corr.scale(0.0), CorrelationMonoid.zero) &&
      corr.scale(1.0) == corr
    }
  }

  property("scaling by a and b is the same as scaling by a*b; similarly for addition") {
    // use Int here instead of doubles so that we don't have to worry about overlfowing to Infinity and having to
    // fine-tune numerical precision thresholds.
    forAll { (corr: Correlation, a0: Int, b0: Int) =>
      val a = a0 & Int.MaxValue
      val b = b0 & Int.MaxValue
      corrApproxEq(corr.scale(a).scale(b), corr.scale(a.toDouble * b)) &&
      corrApproxEq(corr.scale(a.toDouble + b), CorrelationMonoid.plus(corr.scale(a), corr.scale(b)))
    }
  }

  property("adding together scaled correlations is the same as scaling then adding") {
    forAll { (corr1: Correlation, corr2: Correlation, z0: Int) =>
      val z = z0 & Int.MaxValue
      val addThenScale = CorrelationMonoid.plus(corr1, corr2).scale(z)
      val scaleThenAdd = CorrelationMonoid.plus(corr1.scale(z), corr2.scale(z))
      corrApproxEq(addThenScale, scaleThenAdd)
    }
  }

  property("scaling does affect total weight, doesn't affect mean, variance, or correlation") {
    // def sign(x: Int): Int = if (x < 0) -1 else 1
    forAll { (corr: Correlation, a0: Int) =>
      val a = a0 & Int.MaxValue
      val scaled = corr.scale(a.toDouble)
      (a == 0.0) ||
      approxEqOrBothNaN(EPS)(scaled.totalWeight, corr.totalWeight * a) &&
      approxEqOrBothNaN(EPS)(scaled.meanX, corr.meanX) &&
      approxEqOrBothNaN(EPS)(scaled.meanY, corr.meanY) &&
      approxEqOrBothNaN(EPS)(scaled.varianceX, corr.varianceX) &&
      approxEqOrBothNaN(EPS)(scaled.varianceY, corr.varianceY) &&
      approxEqOrBothNaN(EPS)(scaled.correlation, corr.correlation)
    }

  }
}

class CorrelationTest extends AnyWordSpec with Matchers {
  import CorrelationLaws._

  "correlation with y = x*x should be 0" in {
    aggregateFunction(x => x * x)(testList) should equal(0.0)
  }

}
