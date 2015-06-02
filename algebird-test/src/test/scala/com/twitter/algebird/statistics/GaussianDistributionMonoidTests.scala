package com.twitter.algebird

class GaussianDistributionMonoidTests extends CheckProperties {
  import com.twitter.algebird.BaseProperties._
  val MEAN = 10
  val SIGMA = 100

  implicit def gaussianMonoid = new GaussianDistributionMonoid
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