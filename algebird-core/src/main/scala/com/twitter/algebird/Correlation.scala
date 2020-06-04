package com.twitter.algebird

object Correlation {
  def apply(x: (Double, Double)): Correlation =
    Correlation(c2 = 0, m2Left = 0, m2Right = 0, m1Left = x._1, m1Right = x._2, 1L)

  implicit val group: Group[Correlation] = CorrelationGroup
}

case class Correlation(c2: Double, m2Left: Double, m2Right: Double, m1Left: Double, m1Right: Double, m0: Long) {
  def count: Long = m0

  def meanLeft: Double = m2Left

  def meanRight: Double = m2Right

  // useful in calculating variance and stddev
  private[this] def leftMoment: Moments = Moments(m0 = m0, m1 = m1Left, m2 = m2Left, 0, 0)

  private[this] def rightMoment: Moments = Moments(m0 = m0, m1 = m1Right, m2 = m2Right, 0, 0)

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

object CorrelationGroup extends Group[Correlation] {
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

      val m2Left = a.m2Left + b.m2Left + math.pow(deltaLeft, 2) * prodSumRatio // a.count * b.count / count
      val m2Right = a.m2Right + b.m2Right + math.pow(deltaRight, 2) * prodSumRatio // a.count * b.count / count

      val c2 = a.c2 + b.c2 + deltaLeft * deltaRight * prodSumRatio // a.count * b.count / count

      Correlation(c2 = c2, m2Left = m2Left, m2Right = m2Right, m1Left = m1Left, m1Right = m1Right, m0 = count)
    }
  }

  override val zero = Correlation(0, 0, 0, 0, 0, 0)

  override def negate(v: Correlation): Correlation =
    Correlation(c2 = -v.c2, m2Left = -v.m2Left, m2Right = -v.m2Right, m1Left = v.m1Left, m1Right = v.m1Right, m0 = -v.m0)

}