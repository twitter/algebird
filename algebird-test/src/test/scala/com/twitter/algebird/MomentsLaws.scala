package com.twitter.algebird

import org.scalatest._
import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._

class MomentsLaws extends CheckProperties {
  val EPS = 1e-10

  property("Moments Group laws") {
    implicit val equiv: Equiv[Moments] =
      Equiv.fromFunction { (ml, mr) =>
        (ml.m0 == mr.m0) &&
          approxEq(EPS)(ml.m1, mr.m1) &&
          approxEq(EPS)(ml.m2, mr.m2) &&
          approxEq(EPS)(ml.m3, mr.m3) &&
          approxEq(EPS)(ml.m4, mr.m4)
      }
    groupLaws[Moments]
  }
}

class MomentsTest extends WordSpec with Matchers {
  def testApproxEq(f1: Double, f2: Double) {
    if (f2 == 0)
      assert(f1 < 1e-10)
    else
      assert((scala.math.abs(f1 - f2) / scala.math.abs(f2)) < 1e-10)
  }

  /**
   * Given a list of doubles, create a Moments object to hold
   * the list's central moments.
   */
  def getMoments(xs: List[Double]): Moments =
    xs.foldLeft(MomentsGroup.zero) { (m, x) => MomentsGroup.plus(m, Moments(x)) }

  "Moments should count" in {
    val m1 = getMoments(List(1, 2, 3, 4, 5))
    testApproxEq(m1.count, 5)

    val m2 = getMoments(Nil)
    testApproxEq(m2.count, 0)
  }

  "Moments should calculate mean" in {
    val m1 = getMoments(List(1, 2, 3, 4, 5))
    testApproxEq(m1.mean, 3)

    val m2 = getMoments(List(1, 1, 1, 2, 3))
    testApproxEq(m2.mean, 1.6)
  }

  // variance = function(x) { mean((x - mean(x)) ** 2) }
  "Moments should calculate variance" in {
    val m1 = getMoments(List(1, 2, 3, 4, 5))
    testApproxEq(m1.variance, 2)

    val m2 = getMoments(List(1, 1, 1, 2, 3))
    testApproxEq(m2.variance, 0.64)
  }

  // skewness = function(x) { mean((x - mean(x)) ** 3) / (mean((x - mean(x)) ** 2)) ** 1.5 }
  "Moments should calculate skewness" in {
    val m1 = getMoments(List(1, 2, 3, 4, 5))
    testApproxEq(m1.skewness, 0)

    val m2 = getMoments(List(1, 1, 1, 2, 3))
    testApproxEq(m2.skewness, 0.84375)
  }

  // kurtosis = function(x) { -3 + mean((x - mean(x)) ** 4) / (mean((x - mean(x)) ** 2) ** 2) }
  "Moments should calculate kurtosis" in {
    val m1 = getMoments(List(1, 2, 3, 4, 5))
    testApproxEq(m1.kurtosis, -1.3)

    val m2 = getMoments(List(1, 1, 1, 2, 3))
    testApproxEq(m2.kurtosis, -0.921875)
  }

  "Moments can be aggregated" in {
    val m1 = MomentsAggregator(List(1, 2, 3, 4, 5))
    testApproxEq(m1.count, 5)
    testApproxEq(m1.mean, 3)
    testApproxEq(m1.variance, 2)
    testApproxEq(m1.skewness, 0)
    testApproxEq(m1.kurtosis, -1.3)

    val m2 = MomentsAggregator(List(1, 1, 1, 2, 3))
    testApproxEq(m2.count, 5)
    testApproxEq(m2.mean, 1.6)
    testApproxEq(m2.variance, 0.64)
    testApproxEq(m2.skewness, 0.84375)
    testApproxEq(m2.kurtosis, -0.921875)
  }

  "Moments should not return higher-order moments for small data sets" in {
    val m1 = MomentsAggregator(List(1, 2))
    testApproxEq(m1.count, 2)
    assert(m1.skewness.isNaN)
    assert(m1.kurtosis.isNaN)
  }
}
