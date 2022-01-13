package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalacheck.{Arbitrary, Gen, Prop}

class MomentsLaws extends CheckProperties {
  import Prop.forAll

  val EPS: Double = 1e-10

  implicit val equiv: Equiv[Moments] =
    Equiv.fromFunction { (ml, mr) =>
      approxEq(EPS)(ml.m0D, mr.m0D) &&
      approxEq(EPS)(ml.m1, mr.m1) &&
      approxEq(EPS)(ml.m2, mr.m2) &&
      approxEq(EPS)(ml.m3, mr.m3) &&
      approxEq(EPS)(ml.m4, mr.m4)
    }

  def opBasedGen[A: Numeric](genA: Gen[A]): Gen[Moments] = {
    val init: Gen[Moments] = genA.map(Moments(_))

    val recur = Gen.lzy(opBasedGen[A](genA))
    val pair = Gen.zip(recur, recur)

    Gen.frequency(
      (10, init),
      (1, pair.map { case (a, b) => a + b })
    )
  }

  property("Moments Group laws") {
    import com.twitter.algebird.scalacheck.arbitrary.momentsArb
    implicit val group: Group[Moments] = MomentsGroup
    groupLaws[Moments]
  }

  private val opGen: Gen[Moments] =
    opBasedGen[Double](Gen.choose(-1e10, 1e10))

  property("Moments laws tested with operational generation") {
    implicit val arbMom: Arbitrary[Moments] = Arbitrary(opGen)

    monoidLaws[Moments]
  }

  property("scaling by 0 and 1 works as you'd expect") {
    forAll(opGen) { (mom: Moments) =>
      (mom.scale(0.0) == Monoid.zero[Moments]) &&
      mom.scale(1.0) == mom
    }
  }

  property("scaling by a and b is the same as scaling by a*b; similarly for addition") {
    // use Int here instead of doubles so that we don't have to worry about
    // overflowing to Infinity and having to fine-tune numerical precision
    // thresholds.
    forAll(opGen, Gen.choose(0, Int.MaxValue), Gen.choose(0, Int.MaxValue)) { (mom, a0, b0) =>
      val a = a0 & Int.MaxValue
      val b = b0 & Int.MaxValue
      (equiv.equiv(mom.scale(a).scale(b), mom.scale(a.toDouble * b)) &&
      equiv.equiv(mom.scale(a.toDouble + b), Monoid.plus(mom.scale(a), mom.scale(b))))
    }
  }

  property("adding together scaled moments is the same as scaling then adding") {
    forAll(opGen, opGen, Gen.choose(0, Int.MaxValue)) { (mom1, mom2, z0) =>
      val z = z0 & Int.MaxValue
      val addThenScale = Monoid.plus(mom1, mom2).scale(z)
      val scaleThenAdd = Monoid.plus(mom1.scale(z), mom2.scale(z))
      equiv.equiv(addThenScale, scaleThenAdd)
    }
  }

  property("adding double matches adding singleton Moments instance") {
    forAll(opGen, Gen.choose(0, Int.MaxValue)) { (mom, x) =>
      val plusMoments = mom + Moments(x)
      val plusDouble = mom + x
      equiv.equiv(plusMoments, plusDouble)
    }
  }

  property("adding doubles via +, fold, aggregator should match") {
    forAll(opGen, Gen.containerOf[Seq, Double](Gen.choose(0, 1000))) {
      (mom, xs) =>
      val fullViaAdd = xs.foldLeft(mom)(_ + _)
      val fullViaFold = mom.fold.overTraversable(xs)
      val fullViaAgg = mom + MomentsAggregator(xs)

      equiv.equiv(fullViaAdd, fullViaFold)
      equiv.equiv(fullViaAdd, fullViaAgg)
    }
  }

  property("adding Moment instances via +, sumOption should match") {
      forAll(opGen, Gen.containerOf[Seq, Double](Gen.choose(0, 1000))) {
        (mom, ints) =>
        val xs = ints.map(Moments(_)).toTraversable
        val monoid = Moments.momentsMonoid

        val fullViaAdd = xs.foldLeft(mom)(_ + _)
        val fullViaMonoid = mom + monoid.sumOption(xs).getOrElse(monoid.zero)

        equiv.equiv(fullViaAdd, fullViaMonoid)
      }
    }

  property("scaling does affect total weight, doesn't affect mean, variance, or moments") {
    // def sign(x: Int): Int = if (x < 0) -1 else 1
    forAll(opGen, Gen.choose(0, Int.MaxValue)) { (mom, a0) =>
      val a = a0 & Int.MaxValue
      val scaled = mom.scale(a.toDouble)
      (a == 0) || {
        approxEq(EPS)(scaled.totalWeight, mom.totalWeight * a) &&
        approxEq(EPS)(scaled.mean, mom.mean) &&
        approxEq(EPS)(scaled.variance, mom.variance) &&
        approxEqOrBothNaN(EPS)(scaled.skewness, mom.skewness) &&
        approxEqOrBothNaN(EPS)(scaled.kurtosis, mom.kurtosis)
      }
    }

  }
}

class MomentsTest extends AnyWordSpec with Matchers {
  def testApproxEq(f1: Double, f2: Double): Unit =
    assert(approxEq(1e-10)(f1, f2))

  /**
   * Given a list of doubles, create a Moments object to hold the list's central moments.
   */
  def getMoments(xs: List[Double]): Moments =
    Moments.aggregator(xs)

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
}
