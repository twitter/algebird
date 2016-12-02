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

/**
 * Tracks the count and mean value of Doubles in a data stream.
 *
 * Adding two instances of [[AveragedValue]] with [[+]]
 * is equivalent to taking an average of the two streams, with each
 * stream weighted by its count.
 *
 * The mean calculation uses a numerically stable online algorithm
 * suitable for large numbers of records, similar to Chan et. al.'s
 * [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
 * parallel variance algorithm on Wikipedia]]. As long as your count
 * doesn't overflow a Long, the mean calculation won't overflow.
 *
 * @see [[MomentsGroup.getCombinedMean]] for implementation of [[+]]
 * @param count the number of aggregated items
 * @param value the average value of all aggregated items
 */
case class AveragedValue(count: Long, value: Double) {
  /**
   * Returns a copy of this instance with a negative value. Note that
   *
   * {{{
   * a + -b == a - b
   * }}}
   */
  def unary_- : AveragedValue = copy(count = -count)

  /**
   * Averages this instance with the *opposite* of the supplied
   * [[AveragedValue]] instance, effectively subtracting out that
   * instance's contribution to the mean.
   *
   * @param r the instance to subtract
   * @return an instance with `r`'s stream subtracted out
   */
  def -(r: AveragedValue): AveragedValue = AveragedGroup.minus(this, r)

  /**
   * Averages this instance with another [[AveragedValue]] instance.
   * @param r the other instance
   * @return an instance representing the mean of this instance and `r`.
   */
  def +(r: AveragedValue): AveragedValue = AveragedGroup.plus(this, r)
}

/**
 * Provides a set of operations needed to create and use
 * [[AveragedValue]] instances.
 */
object AveragedValue {
  /** implicit instance of [[Group]][AveragedValue] */
  implicit val group = AveragedGroup

  /**
   * Returns an [[Aggregator]] that uses [[AveragedValue]] to
   * calculate the mean of all `Double` values in the stream. Each
   * Double value receives a count of 1 during aggregation.
   */
  def aggregator: Aggregator[Double, AveragedValue, Double] = Averager

  /**
   * Returns an [[Aggregator]] that uses [[AveragedValue]] to
   * calculate the mean of all values in the stream. Each numeric
   * value receives a count of `1` during aggregation.
   *
   * @tparam N numeric type to convert into `Double`
   */
  def numericAggregator[N](implicit num: Numeric[N]): MonoidAggregator[N, AveragedValue, Double] =
    Aggregator.prepareMonoid { n: N => AveragedValue(num.toDouble(n)) }
      .andThenPresent(_.value)

  /**
   * Creates [[AveragedValue]] with a value of `v` and a count of 1.
   *
   * @tparam V type with an implicit conversion to Double
   */
  def apply[V <% Double](v: V): AveragedValue = apply(1L, v)

  /**
   * Creates an [[AveragedValue]] with a count of of `c` and a value
   * of `v`.
   *
   * @tparam V type with an implicit conversion to Double
   */
  def apply[V <% Double](c: Long, v: V): AveragedValue = new AveragedValue(c, v)
}

/**
 * [[Group]] implementation for [[AveragedValue]].
 *
 * @define T `AveragedValue`
 */
object AveragedGroup extends Group[AveragedValue] {
  import MomentsGroup.getCombinedMean

  val zero = AveragedValue(0L, 0.0)

  override def isNonZero(av: AveragedValue) = (av.count != 0L)

  override def negate(av: AveragedValue) = -av

  /**
   * Optimized implementation of [[plus]]. Uses internal mutation to
   * combine the supplied [[AveragedValue]] instances without creating
   * intermediate objects.
   */
  override def sumOption(iter: TraversableOnce[AveragedValue]): Option[AveragedValue] =
    if (iter.isEmpty) None
    else {
      var count = 0L
      var average = 0.0
      iter.foreach {
        case AveragedValue(c, v) =>
          average = getCombinedMean(count, average, c, v)
          count += c
      }
      Some(AveragedValue(count, average))
    }

  /**
   * @inheritdoc
   * @see [[AveragedValue.+]] for the implementation
   */
  def plus(l: AveragedValue, r: AveragedValue): AveragedValue = {
    val n = l.count
    val k = r.count
    val newAve = getCombinedMean(n, l.value, k, r.value)
    AveragedValue(n + k, newAve)
  }
}

/**
 * [[Aggregator]] that uses [[AveragedValue]] to calculate the mean
 * of all `Double` values in the stream. Each Double value receives a
 * count of 1 during aggregation.
 */
object Averager extends MonoidAggregator[Double, AveragedValue, Double] {
  val monoid = AveragedGroup
  def prepare(value: Double) = AveragedValue(value)
  def present(average: AveragedValue) = average.value
}
