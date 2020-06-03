package com.twitter.algebird

case class Correlation(c2: Double, m2Left: Double, m2Right: Double, m1Left: Double, m1Right: Double, m0: Long) {
  def count: Long = m0
  def meanLeft: Double =  m2Left
  def meanRight: Double = m2Right
  // useful in calculating variance and stddev
  private[this] def leftMoment: Moments = Moments(m0, m1Left, m2Left, 0, 0)
  private[this] def rightMoment: Moments = Moments(m0, m1Right, m2Right)

  // variance, stddev, covariance, and correlation are for the population, not a sample
  def varianceLeft: Double = leftMoment.variance
  def varianceRight: Double = rightMoment.variance

  def stddevLeft: Double = leftMoment.stddev
  def stddevRight: Double = rightMoment.stddev

  def covariance: Double = c2 / count

  // pearson's correlation coefficient

  def correlation =
    // correlation is defined as: covariance / (varianceLeft * varianceRight)
    // however, dividing by "count" cancels out, and leaves us with the following formula, which relies on fewer
    // divisions
    c2 / (Math.sqrt(m2Left * m2Right))


}
