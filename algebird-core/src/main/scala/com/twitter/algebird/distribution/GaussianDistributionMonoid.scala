package com.twitter.algebird

import com.twitter.algebird.Monoid

/**
 * @mean - Mean
 * @sigma - Standard deviation
 * aka Normal distribution
 */
case class GaussianDistribution(mean: Double, sigma: Double)

object GaussianDistribution {
  implicit def monoid: Monoid[GaussianDistribution] = GaussianDistributionMonoid
}

/**
 * This monoid stems from the fact that if X and Y are independent random variables
 * that are normally distributed, then their sum is also
 * normally distributed, with its new mean equal to the sum of two means
 * and variance equal to the sum of two variances.
 */
class GaussianDistributionMonoid extends Monoid[GaussianDistribution] {
  override def zero = new GaussianDistribution(0, 0)

  override def plus(left: GaussianDistribution, right: GaussianDistribution): GaussianDistribution = {
    new GaussianDistribution(left.mean + right + mean, left.sigma + right.sigma)
  }
}