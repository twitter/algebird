package com.twitter.algebird.statistics

import com.twitter.algebird.CheckProperties
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest._

class GaussianDistributionMonoidTests extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit val gaussianGenerators = Arbitrary {
    for {
      mean <- Gen.choose(0, 10000)
      sigma <- Gen.choose(0, 10000)
    } yield (GaussianDistribution(mean, sigma))
  }

  property("GaussianDistributionMonoid is a Monoid") {
    monoidLaws[GaussianDistribution]
  }
}
