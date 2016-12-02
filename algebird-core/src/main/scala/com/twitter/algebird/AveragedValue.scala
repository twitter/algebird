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
 * AveragedValue tracks the count and mean, or average value, of a
 * data stream.
 *
 * Adding two instances of AveragedValue with `+` is equivalent to
 * taking an average of the two streams, with each stream weighted by
 * its count.
 *
 * The mean calculation uses an online algorithm suitable for large
 * numbers of records, similar to:
 * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
 *
 * See [[MomentsGroup.getCombinedMean]] for the code.
 *
 * @param count the number of items aggregated into this instance
 * @param value the average value of all items aggregated into this instance
 */
case class AveragedValue(count: Long, value: Double) {
  def unary_- : AveragedValue = copy(count = -count)
  def -(r: AveragedValue): AveragedValue = AveragedGroup.minus(this, r)
  def +(r: AveragedValue): AveragedValue = AveragedGroup.plus(this, r)
}

object AveragedValue {
  // TODO: Change to Group[AveragedValue] in 0.13.0 (https://github.com/twitter/algebird/issues/587)
  implicit val group = AveragedGroup

  def aggregator: Aggregator[Double, AveragedValue, Double] = Averager

  def numericAggregator[N](implicit num: Numeric[N]): MonoidAggregator[N, AveragedValue, Double] =
    Aggregator.prepareMonoid { n: N => AveragedValue(num.toDouble(n)) }
      .andThenPresent(_.value)

  def apply[V <% Double](v: V): AveragedValue = apply(1L, v)
  def apply[V <% Double](c: Long, v: V): AveragedValue = new AveragedValue(c, v)
}

object AveragedGroup extends Group[AveragedValue] {
  import MomentsGroup.getCombinedMean

  val zero = AveragedValue(0L, 0.0)

  override def isNonZero(av: AveragedValue) = (av.count != 0L)

  override def negate(av: AveragedValue) = -av

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

  def plus(left: AveragedValue, right: AveragedValue): AveragedValue = {
    val n = left.count
    val k = right.count
    val newAve = getCombinedMean(n, left.value, k, right.value)
    AveragedValue(n + k, newAve)
  }
}

object Averager extends MonoidAggregator[Double, AveragedValue, Double] {
  val monoid = AveragedGroup
  def prepare(value: Double) = AveragedValue(value)
  def present(average: AveragedValue) = average.value
}
