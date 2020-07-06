package com.twitter.algebird

object Correlation {
  def apply(x: (Double, Double), weight: Double): Correlation =
    Correlation(c2 = 0, m2Left = 0, m2Right = 0, m1Left = x._1, m1Right = x._2, weight)

  def apply(x: (Double, Double)): Correlation =
    apply(x, 1.0)

  implicit val group: Group[Correlation] = CorrelationGroup

  /**
   * When combining averages, if the counts sizes are too close we
   * should use a different algorithm.  This constant defines how
   * close the ratio of the smaller to the total count can be:
   */
  private val STABILITY_CONSTANT = 0.1

  /**
   * Given two streams of doubles (weightN, an) and (weightK, ak) of form (weighted count,
   * mean), calculates the mean of the combined stream.
   *
   * Uses a more stable online algorithm which should be suitable for
   * large numbers of records similar to:
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   *
   * This differs from the implementation in MomentsGroup.scala only in that here, the counts are weighted, and are
   * thus doubles instead of longs
   */
  def getCombinedMean(weightN: Double, an: Double, weightK: Double, ak: Double): Double =
    if (weightN < weightK) getCombinedMean(weightK, ak, weightN, an)
    else
      (weightN + weightK) match {
        case 0L                              => 0.0
        case newCount if newCount == weightN => an
        case newCount =>
          val scaling = weightK.toDouble / newCount
          // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
          if (scaling < STABILITY_CONSTANT) (an + (ak - an) * scaling)
          else (weightN * an + weightK * ak) / newCount
      }

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
    m0: Double
) {
  def totalWeight: Double = m0

  def meanLeft: Double = m1Left

  def meanRight: Double = m1Right

  // variance, stddev, and covariance are for the population, not a sample

  def varianceLeft: Double = m2Left / m0

  def varianceRight: Double = m2Right / m0

  def stddevLeft: Double = Math.sqrt(varianceLeft)

  def stddevRight: Double = Math.sqrt(varianceRight)

  def covariance: Double = c2 / totalWeight

  /**
   *
   * @return Pearson's correlation coefficient
   */
  def correlation: Double =
    // correlation is defined as: covariance / (varianceLeft * varianceRight)
    // however, dividing by "count" cancels out, and leaves us with the following formula, which relies on fewer
    // divisions
    c2 / (Math.sqrt(m2Left * m2Right))

  /**
   * Assume this instance of Correlation came from summing together Correlation.apply((x_i, y_i)) for i in 1...n.
   * @return (m, b) where y = mx + b is the line with the least squares fit of the points (x_i, y_i).
   *         See, e.g. https://mathworld.wolfram.com/LeastSquaresFitting.html.
   */
  def linearLeastSquares: (Double, Double) = {
    val m = c2 / m2Left
    val b = meanRight - m * meanLeft
    (m, b)
  }

  def swap: Correlation =
    Correlation(c2 = c2, m2Left = m2Right, m2Right = m2Left, m1Left = m1Right, m1Right = m1Left, m0 = m0)
}

object CorrelationGroup extends Group[Correlation] {

  /**
   * The algorithm for combining the correlation calculations from two partitions of pairs of numbers. Comes from
   * Pébay, Philippe (2008), "Formulas for Robust, One-Pass Parallel Computation of Covariances and Arbitrary-Order Statistical Moments",
   *   Technical Report SAND2008-6212, Sandia National Laboratories
   * https://prod-ng.sandia.gov/techlib-noauth/access-control.cgi/2008/086212.pdf
   *
   * Extending this to weights can be found in
   * Schubert, Erich; Gertz, Michael (9 July 2018). Numerically stable parallel computation of (co-)variance.
   *   ACM. p. 10. doi:10.1145/3221269.3223036. ISBN 9781450365055.
   *   http://dl.acm.org/citation.cfm?id=3221269.3223036
   *   https://dl.acm.org/doi/10.1145/3221269.3223036
   */
  override def plus(a: Correlation, b: Correlation): Correlation = {
    val count = a.totalWeight + b.totalWeight
    if (count == 0)
      zero
    else {
      val prodSumRatio = a.totalWeight * b.totalWeight / count
      val m1Left = Correlation.getCombinedMean(a.totalWeight, a.m1Left, b.totalWeight, b.m1Left)
      val m1Right = Correlation.getCombinedMean(a.totalWeight, a.m1Right, b.totalWeight, b.m1Right)
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

  def correlation: MonoidAggregator[(Double, Double), Correlation, Double] =
    this.andThenPresent(_.correlation)

  def covariance: MonoidAggregator[(Double, Double), Correlation, Double] =
    this.andThenPresent(_.covariance)

}
