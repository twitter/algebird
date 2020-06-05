package com.twitter.algebird

object Correlation {
  def apply(x: (Double, Double)): Correlation =
    Correlation(c2 = 0, m2Left = 0, m2Right = 0, m1Left = x._1, m1Right = x._2, 1L)

  implicit val group: Group[Correlation] = CorrelationGroup
}

/**
 * A class to calculate covariance and the first two central moments of a sequence of pairs of Doubles, from which the
 * pearson correlation coeifficient can be calculated.
 *
 * m{i}Left denotes the ith central moment of the first projection of the pair.
 * m{i}Right denotes the ith central moment of the second projection of the pair.
 * c2 the covariance equivalent of the second central moment, i.e. c2 = Sum_(l,r) (l - m1Left)*(r - m1Right).
 *
 */
case class Correlation(
    c2: Double,
    m2Left: Double,
    m2Right: Double,
    m1Left: Double,
    m1Right: Double,
    m0: Long
) {
  def count: Long = m0

  def meanLeft: Double = m1Left

  def meanRight: Double = m1Right

  // useful in calculating variance and stddev; private because we don't have skewness or kurtosis info
  private[algebird] def leftMoment: Moments = Moments(m0 = m0, m1 = m1Left, m2 = m2Left, 0, 0)

  private[algebird] def rightMoment: Moments = Moments(m0 = m0, m1 = m1Right, m2 = m2Right, 0, 0)

  // variance, stddev, and covariance are for the population, not a sample
  def varianceLeft: Double = leftMoment.variance

  def varianceRight: Double = rightMoment.variance

  def stddevLeft: Double = leftMoment.stddev

  def stddevRight: Double = rightMoment.stddev

  def covariance: Double = c2 / count

  /**
   *
   * @return Pearson's correlation coefficient
   */
  def correlation: Double =
    // correlation is defined as: covariance / (varianceLeft * varianceRight)
    // however, dividing by "count" cancels out, and leaves us with the following formula, which relies on fewer
    // divisions
    c2 / (Math.sqrt(m2Left * m2Right))
}

object CorrelationGroup extends Group[Correlation] {

  /**
   * The algorithm for combining the correlation calculations from two partitions of pairs of numbers. Comes from
   * Pébay, Philippe (2008), "Formulas for Robust, One-Pass Parallel Computation of Covariances and Arbitrary-Order Statistical Moments",
   *   Technical Report SAND2008-6212, Sandia National Laboratories
   * https://prod-ng.sandia.gov/techlib-noauth/access-control.cgi/2008/086212.pdf
   */
  override def plus(a: Correlation, b: Correlation): Correlation = {
    val count = a.count + b.count
    if (count == 0)
      zero
    else {
      val prodSumRatio = a.count.toDouble * b.count / count
      val m1Left = MomentsGroup.getCombinedMean(a.count, a.m1Left, b.count, b.m1Left)
      val m1Right = MomentsGroup.getCombinedMean(a.count, a.m1Right, b.count, b.m1Right)
      val deltaLeft = b.m1Left - a.m1Left
      val deltaRight = b.m1Right - a.m1Right

      val m2Left = a.m2Left + b.m2Left + math.pow(deltaLeft, 2) * prodSumRatio
      val m2Right =
        a.m2Right + b.m2Right + math.pow(deltaRight, 2) * prodSumRatio

      val c2 = a.c2 + b.c2 + deltaLeft * deltaRight * prodSumRatio

      Correlation(c2 = c2, m2Left = m2Left, m2Right = m2Right, m1Left = m1Left, m1Right = m1Right, m0 = count)
    }
  }

  override val zero = Correlation(0, 0, 0, 0, 0, 0)

  override def negate(v: Correlation): Correlation =
    Correlation(
      c2 = -v.c2,
      m2Left = -v.m2Left,
      m2Right = -v.m2Right,
      m1Left = v.m1Left,
      m1Right = v.m1Right,
      m0 = -v.m0
    )

}

object CorrelationAggregator extends MonoidAggregator[(Double, Double), Correlation, Correlation] {
  override def prepare(a: (Double, Double)): Correlation = Correlation(a)
  override val monoid = CorrelationGroup
  override def present(c: Correlation): Correlation = c
}
