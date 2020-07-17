package com.twitter.algebird

object Correlation {
  def apply(x: (Double, Double), weight: Double): Correlation =
    Correlation(c2 = 0, m2x = 0, m2y = 0, m1x = x._1, m1y = x._2, weight)

  def apply(x: (Double, Double)): Correlation =
    apply(x, 1.0)

  implicit val monoid: Monoid[Correlation] = CorrelationMonoid

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
        case 0.0                             => 0.0
        case newCount if newCount == weightN => an
        case newCount =>
          val scaling = weightK / newCount
          // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
          if (scaling < STABILITY_CONSTANT) (an + (ak - an) * scaling)
          else (weightN * an + weightK * ak) / newCount
      }

}

/**
 * A class to calculate covariance and the first two central moments of a sequence of pairs of Doubles, from which the
 * pearson correlation coeifficient can be calculated.
 *
 * m{i}x denotes the ith central moment of the first projection of the pair.
 * m{i}y denotes the ith central moment of the second projection of the pair.
 * c2 the covariance equivalent of the second central moment, i.e. c2 = Sum_(x,y) (x - m1x)*(y - m1y).
 */
case class Correlation(c2: Double, m2x: Double, m2y: Double, m1x: Double, m1y: Double, m0: Double) {
  def totalWeight: Double = m0

  def meanX: Double = m1x

  def meanY: Double = m1y

  // variance, stddev, and covariance are for the population, not a sample

  def varianceX: Double = m2x / m0

  def varianceY: Double = m2y / m0

  def stddevX: Double = Math.sqrt(varianceX)

  def stddevY: Double = Math.sqrt(varianceY)

  def covariance: Double = c2 / totalWeight

  /**
   * @return Pearson's correlation coefficient
   */
  def correlation: Double =
    // correlation is defined as: covariance / (varianceLeft * varianceRight)
    // however, dividing by "count" cancels out, and leaves us with the following formula, which relies on fewer
    // divisions
    c2 / (Math.sqrt(m2x * m2y))

  /**
   * Assume this instance of Correlation came from summing together Correlation.apply((x_i, y_i)) for i in 1...n.
   *
   * @return (m, b) where y = mx + b is the line with the least squares fit of the points (x_i, y_i).
   *         See, e.g. https://mathworld.wolfram.com/LeastSquaresFitting.html.
   */
  def linearLeastSquares: (Double, Double) = {
    val m = c2 / m2x
    val b = meanY - m * meanX
    (m, b)
  }

  def swap: Correlation =
    Correlation(c2 = c2, m2x = m2y, m2y = m2x, m1x = m1y, m1y = m1x, m0 = m0)

  def distanceMetric: Double = math.sqrt(1.0 - correlation)

  def scale(z: Double): Correlation =
    if (z < 0.0) // the "extraneous" if here is to avoid allocating the error message unless necessary
      throw new IllegalArgumentException(s"cannot scale by negative value: $z")
    else if (z == 0)
      CorrelationMonoid.zero
    else
      Correlation(c2 = z * c2, m2x = z * m2x, m2y = z * m2y, m1x = m1x, m1y = m1y, m0 = z * m0)
}

object CorrelationMonoid extends Monoid[Correlation] {

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
    if (count == 0.0)
      CorrelationMonoid.zero
    else {
      val prodSumRatio = a.totalWeight * b.totalWeight / count

      val m1x = Correlation.getCombinedMean(a.totalWeight, a.m1x, b.totalWeight, b.m1x)
      val m1y = Correlation.getCombinedMean(a.totalWeight, a.m1y, b.totalWeight, b.m1y)
      val deltaX = b.m1x - a.m1x
      val deltaY = b.m1y - a.m1y

      val m2x = a.m2x + b.m2x + math.pow(deltaX, 2) * prodSumRatio
      val m2y =
        a.m2y + b.m2y + math.pow(deltaY, 2) * prodSumRatio

      val c2 = a.c2 + b.c2 + deltaX * deltaY * prodSumRatio

      Correlation(c2 = c2, m2x = m2x, m2y = m2y, m1x = m1x, m1y = m1y, m0 = count)
    }
  }

  override val zero = Correlation(0, 0, 0, 0, 0, 0)

  override def sumOption(cs: TraversableOnce[Correlation]): Option[Correlation] =
    if (cs.isEmpty) None
    else {
      val iter = cs.toIterator
      val item = iter.next()

      var m0 = item.m0
      var m1y = item.m1y
      var m1x = item.m1x
      var m2y = item.m2y
      var m2x = item.m2x
      var c2 = item.c2

      while (iter.hasNext) {

        /**
         * This is tested by monoidLaws to match plus
         * we do this loop here to avoid allocating
         * between each pair of Correlations
         */
        val b = iter.next()
        val m0New = m0 + b.m0

        if (m0New == 0.0) {
          m1y = 0.0
          m1x = 0.0
          m2y = 0.0
          m2x = 0.0
          c2 = 0.0
        } else {
          val prodSumRatio = m0 * b.m0 / m0New

          val m1xNew = Correlation.getCombinedMean(m0, m1x, b.m0, b.m1x)
          val m1yNew = Correlation.getCombinedMean(m0, m1y, b.m0, b.m1y)
          val deltaX = b.m1x - m1x
          val deltaY = b.m1y - m1y

          val m2xNew = m2x + b.m2x + math.pow(deltaX, 2) * prodSumRatio
          val m2yNew =
            m2y + b.m2y + math.pow(deltaY, 2) * prodSumRatio

          val c2New = c2 + b.c2 + deltaX * deltaY * prodSumRatio

          m1y = m1yNew
          m1x = m1xNew
          m2y = m2yNew
          m2x = m2xNew
          c2 = c2New
        }
        m0 = m0New
      }

      if (m0 == 0.0) Some(zero)
      else Some(Correlation(c2 = c2, m2x = m2x, m2y = m2y, m1x = m1x, m1y = m1y, m0 = m0))
    }
}

object CorrelationAggregator extends MonoidAggregator[(Double, Double), Correlation, Correlation] {
  override def prepare(a: (Double, Double)): Correlation = Correlation(a)
  override def monoid: Monoid[Correlation] = CorrelationMonoid
  override def present(c: Correlation): Correlation = c

  def correlation: MonoidAggregator[(Double, Double), Correlation, Double] =
    this.andThenPresent(_.correlation)

  def covariance: MonoidAggregator[(Double, Double), Correlation, Double] =
    this.andThenPresent(_.covariance)

}
