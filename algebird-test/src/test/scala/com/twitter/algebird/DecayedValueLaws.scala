package com.twitter.algebird

import org.scalacheck.Arbitrary
import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.scalacheck.arbitrary._
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._
import org.scalacheck.Prop

class DecayedValueLaws extends CheckProperties {
  val EPS: Double = 0.1

  case class Params(mean: Double, halfLife: Double, count: Int, maxNoise: Double)

  implicit val decayedMonoid: Monoid[DecayedValue] = DecayedValue.monoidWithEpsilon(0.001)

  property("DecayedValue Monoid laws") {
    implicit val equiv: Equiv[DecayedValue] =
      Equiv.fromFunction { (dvl, dvr) =>
        approxEq(EPS)(dvl.value, dvr.value) && (dvl.scaledTime == dvr.scaledTime)
      }
    commutativeMonoidLaws[DecayedValue]
  }

  def averageApproxEq(fn: (DecayedValue, Params) => Double)(implicit p: Arbitrary[Params]): Prop =
    forAll { (params: Params) =>
      val rand = new scala.util.Random
      val data = (0 to params.count).map { t =>
        val noise = rand.nextDouble * params.maxNoise * rand.nextInt.signum
        DecayedValue.build(params.mean + (params.mean * noise), t, params.halfLife)
      }
      val result = decayedMonoid.sum(data)
      approxEq(EPS)(fn(result, params), params.mean)
    }

  property("for large HL and count, average(f(t)=x)=x") {
    implicit val params: Arbitrary[Params] = Arbitrary {
      for {
        x <- choose(-1e100, 1e100)
        hl <- choose(100.0, 1000.0)
        c <- choose(10000, 100000)
        n <- choose(0.0, 0.1)
      } yield Params(x, hl, c, n)
    }

    averageApproxEq((dv, params) => dv.average(params.halfLife))
  }

  property("for large HL but small count, averageFrom(f(t)=x)=x") {
    implicit val params: Arbitrary[Params] = Arbitrary {
      for {
        x <- choose(-1e100, 1e100)
        hl <- choose(100.0, 1000.0)
        c <- choose(20, 1000)
        n <- choose(0.0, 0.1)
      } yield Params(x, hl, c, n)
    }

    averageApproxEq((dv, params) => dv.averageFrom(params.halfLife, 0, params.count))
  }

  property("for small HL but large count, discreteAverage(f(t)=x)=x") {
    implicit val params: Arbitrary[Params] = Arbitrary {
      for {
        x <- choose(-1e100, 1e100)
        hl <- choose(1.0, 10.0)
        c <- choose(10000, 100000)
        n <- choose(0.0, 0.1)
      } yield Params(x, hl, c, n)
    }

    averageApproxEq((dv, params) => dv.discreteAverage(params.halfLife))
  }
}
