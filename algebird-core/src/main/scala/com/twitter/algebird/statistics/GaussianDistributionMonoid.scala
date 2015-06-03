package com.twitter.algebird.statistics

import com.twitter.algebird.Monoid

/**
 * @mean - Mean
 * @sigma - Standard deviation
 * aka Normal distribution
 */
case class GaussianDistribution(mean: Double, sigma: Double)

object GaussianDistribution {
  implicit val monoid: Monoid[GaussianDistribution] = GaussianDistributionMonoid

  def sample(r: java.util.Random): Double =
    r.nextGaussian()
}

/**
 * This monoid stems from the fact that if X and Y are independent random variables
 * that are normally distributed, then their sum is also
 * normally distributed, with its new mean equal to the sum of two means
 * and variance equal to the sum of two variances.
 */
object GaussianDistributionMonoid extends Monoid[GaussianDistribution] {
  override def zero = new GaussianDistribution(0, 0)

  override def plus(left: GaussianDistribution, right: GaussianDistribution): GaussianDistribution =
    new GaussianDistribution(left.mean + right.mean, left.sigma + right.sigma)

  override def sumOption(its: TraversableOnce[GaussianDistribution]): Option[GaussianDistribution] =
    if (its.isEmpty)
      None
    else {
      var mean = 0.0
      var sigma = 0.0
      val it = its.toIterator
      while (it.hasNext) {
        val g = it.next
        mean += g.mean
        sigma += g.sigma
      }
      Some(GaussianDistribution(mean, sigma))
    }
}