package com.twitter.algebird

import org.scalacheck.{ Arbitrary, Gen }
import org.scalacheck.Prop._

class DecayedValueLaws extends CheckProperties {
  import org.scalacheck.Gen.choose

  def approxEq(f1: Double, f2: Double) = {
    (scala.math.abs(f1 - f2) / scala.math.abs(f2)) < 0.1
  }

  def averageApproxEq(fn: (DecayedValue, Params) => Double)(implicit p: Arbitrary[Params]) = {
    forAll{ (params: Params) =>
      val rand = new scala.util.Random
      val data = (0 to params.count).map{ t =>
        val noise = rand.nextDouble * params.maxNoise * rand.nextInt.signum
        DecayedValue.build(params.mean + (params.mean * noise), t, params.halfLife)
      }
      val result = decayedHasAdditionOperatorAndZero.sum(data)
      approxEq(fn(result, params), params.mean)
    }
  }

  implicit val decayedHasAdditionOperatorAndZero = DecayedValue.monoidWithEpsilon(0.001)
  case class Params(mean: Double, halfLife: Double, count: Int, maxNoise: Double)

  property("for large HL and count, average(f(t)=x)=x") {
    implicit val params: Arbitrary[Params] = Arbitrary{
      for (
        x <- choose(-1e100, 1e100);
        hl <- choose(100.0, 1000.0);
        c <- choose(10000, 100000);
        n <- choose(0.0, 0.1)
      ) yield Params(x, hl, c, n)
    }

    averageApproxEq{ (dv, params) => dv.average(params.halfLife) }
  }

  property("for large HL but small count, averageFrom(f(t)=x)=x") {
    implicit val params: Arbitrary[Params] = Arbitrary{
      for (
        x <- choose(-1e100, 1e100);
        hl <- choose(100.0, 1000.0);
        c <- choose(20, 1000);
        n <- choose(0.0, 0.1)
      ) yield Params(x, hl, c, n)
    }

    averageApproxEq{ (dv, params) => dv.averageFrom(params.halfLife, 0, params.count) }
  }

  property("for small HL but large count, discreteAverage(f(t)=x)=x") {
    implicit val params: Arbitrary[Params] = Arbitrary{
      for (
        x <- choose(-1e100, 1e100);
        hl <- choose(1.0, 10.0);
        c <- choose(10000, 100000);
        n <- choose(0.0, 0.1)
      ) yield Params(x, hl, c, n)
    }

    averageApproxEq{ (dv, params) => dv.discreteAverage(params.halfLife) }
  }
}
