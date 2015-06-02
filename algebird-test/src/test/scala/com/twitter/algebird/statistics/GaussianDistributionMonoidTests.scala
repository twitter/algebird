package com.twitter.algebird.statistics

import com.twitter.algebird.CheckProperties
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest._

class GaussianDistributionMonoidTests extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit val gaussianMonoid = new GaussianDistributionMonoid
  implicit val gaussianGenerators = Arbitrary {
    for (
      mean <- Gen.choose(0, 10000);
      sigma <- Gen.choose(0, 10000)
    ) yield (GaussianDistribution(mean, sigma))
  }

  property("GaussianDistributionMonoid is a Monoid") {
    monoidLawsEq[GaussianDistribution]{
      (a, b) => a.mean == b.mean && a.sigma == b.sigma
    }
  }
}