/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.twitter.algebird

import algebra.CommutativeGroup

/**
 * A class to calculate the first five central moments over a sequence of Doubles.
 * Given the first five central moments, we can then calculate metrics like skewness
 * and kurtosis.
 *
 * m{i} denotes the ith central moment.
 */
case class Moments(m0: Long, m1: Double, m2: Double, m3: Double, m4: Double) {
  def count: Long = m0

  def mean: Double = m1

  // Population variance, not sample variance.
  def variance: Double =
    if (count > 1)
      m2 / count
    else
      /* don't return junk when the moment is not defined */
      Double.NaN

  // Population standard deviation, not sample standard deviation.
  def stddev: Double = math.sqrt(variance)

  def skewness: Double =
    if (count > 2)
      math.sqrt(count) * m3 / math.pow(m2, 1.5)
    else
      /* don't return junk when the moment is not defined */
      Double.NaN

  def kurtosis: Double =
    if (count > 3)
      count * m4 / math.pow(m2, 2) - 3
    else
      /* don't return junk when the moment is not defined */
      Double.NaN
}

object Moments {
  implicit val group: Group[Moments] with CommutativeGroup[Moments] =
    MomentsGroup
  val aggregator: MomentsAggregator.type = MomentsAggregator

  def numericAggregator[N](implicit num: Numeric[N]): MonoidAggregator[N, Moments, Moments] =
    Aggregator.prepareMonoid { n: N => Moments(num.toDouble(n)) }

  // Create a Moments object given a single value. This is useful for
  // initializing moment calculations at the start of a stream.
  def apply[V: Numeric](value: V)(implicit num: Numeric[V]): Moments =
    apply(1L, num.toDouble(value), 0, 0, 0)

  def apply[V](m0: Long, m1: V, m2: V, m3: V, m4: V)(implicit num: Numeric[V]): Moments =
    new Moments(m0, num.toDouble(m1), num.toDouble(m2), num.toDouble(m3), num.toDouble(m4))
}

/**
 * A monoid to perform moment calculations.
 */
object MomentsGroup extends Group[Moments] with CommutativeGroup[Moments] {

  /**
   * When combining averages, if the counts sizes are too close we
   * should use a different algorithm.  This constant defines how
   * close the ratio of the smaller to the total count can be:
   */
  private val STABILITY_CONSTANT = 0.1

  /**
   * Given two streams of doubles (n, an) and (k, ak) of form (count,
   * mean), calculates the mean of the combined stream.
   *
   * Uses a more stable online algorithm which should be suitable for
   * large numbers of records similar to:
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   */
  def getCombinedMean(n: Long, an: Double, k: Long, ak: Double): Double =
    if (n < k) getCombinedMean(k, ak, n, an)
    else
      (n + k) match {
        case 0L                        => 0.0
        case newCount if newCount == n => an
        case newCount =>
          val scaling = k.toDouble / newCount
          // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
          if (scaling < STABILITY_CONSTANT) (an + (ak - an) * scaling)
          else (n * an + k * ak) / newCount
      }

  override val zero: Moments = Moments(0L, 0.0, 0.0, 0.0, 0.0)

  override def negate(a: Moments): Moments =
    Moments(-a.count, a.m1, -a.m2, -a.m3, -a.m4)

  // Combines the moment calculations from two streams.
  // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
  // for more information on the formulas used to update the moments.
  override def plus(a: Moments, b: Moments): Moments = {
    val delta = b.mean - a.mean
    val countCombined = a.count + b.count
    if (countCombined == 0)
      return zero
    val meanCombined = getCombinedMean(a.count, a.mean, b.count, b.mean)

    val m2 = a.m2 + b.m2 +
      math.pow(delta, 2) * a.count * b.count / countCombined

    val m3 = a.m3 + b.m3 +
      math.pow(delta, 3) * a.count * b.count * (a.count - b.count) / math.pow(countCombined, 2) +
      3 * delta * (a.count * b.m2 - b.count * a.m2) / countCombined

    val m4 = a.m4 + b.m4 +
      math.pow(delta, 4) * a.count * b.count * (math.pow(a.count, 2) -
        a.count * b.count + math.pow(b.count, 2)) / math.pow(countCombined, 3) +
      6 * math.pow(delta, 2) * (math.pow(a.count, 2) * b.m2 +
        math.pow(b.count, 2) * a.m2) / math.pow(countCombined, 2) +
      4 * delta * (a.count * b.m3 - b.count * a.m3) / countCombined

    Moments(countCombined, meanCombined, m2, m3, m4)
  }
}

object MomentsAggregator extends MonoidAggregator[Double, Moments, Moments] {
  override val monoid: MomentsGroup.type = MomentsGroup

  override def prepare(input: Double): Moments = Moments(input)
  override def present(m: Moments): Moments = m
}
