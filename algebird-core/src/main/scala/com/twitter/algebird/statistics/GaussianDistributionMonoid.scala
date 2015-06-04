package com.twitter.algebird.statistics

import com.twitter.algebird.Monoid

/**
 * @mean - Mean
 * @sigma2 - Variance, where sqrt(sigma2) is the standard deviation
 * aka Normal distribution
 */
case class GaussianDistribution(mean: Double, sigma2: Double) {
  def stddev: Double = math.sqrt(sigma2)

  def sample(r: java.util.Random): Double =
    mean + r.nextGaussian() * stddev
}

object GaussianDistribution {
  implicit val monoid: Monoid[GaussianDistribution] = GaussianDistributionMonoid
}

/**
 * This monoid stems from the fact that if X and Y are independent random variables
 * that are normally distributed, then their sum is also
 * normally distributed, with its new mean equal to the sum of two means
 * and variance equal to the sum of two variances.
 * http://en.wikipedia.org/wiki/Sum_of_normally_distributed_random_variables
 */
object GaussianDistributionMonoid extends Monoid[GaussianDistribution] {
  override def zero = new GaussianDistribution(0, 0)

  override def plus(left: GaussianDistribution, right: GaussianDistribution): GaussianDistribution =
    new GaussianDistribution(left.mean + right.mean, left.sigma2 + right.sigma2)

  override def sumOption(its: TraversableOnce[GaussianDistribution]): Option[GaussianDistribution] =
    if (its.isEmpty)
      None
    else {
      var mean = 0.0
      var sigma2 = 0.0
      val it = its.toIterator
      while (it.hasNext) {
        val g = it.next
        mean += g.mean
        sigma2 += g.sigma2
      }
      Some(GaussianDistribution(mean, sigma2))
    }
}