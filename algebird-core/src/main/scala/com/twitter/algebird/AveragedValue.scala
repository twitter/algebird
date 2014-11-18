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

object AveragedValue {
  implicit val group = AveragedGroup
  def aggregator: Aggregator[Double, Double] = Averager

  def apply[V <% Double](v: V) = new AveragedValue(1L, v)
  def apply[V <% Double](c: Long, v: V) = new AveragedValue(c, v)
}

case class AveragedValue(count: Long, value: Double)

object AveragedGroup extends Group[AveragedValue] {
  // When combining averages, if the counts sizes are too close we should use a different
  // algorithm.  This constant defines how close the ratio of the smaller to the total count
  // can be:
  private val STABILITY_CONSTANT = 0.1
  /**
   * Uses a more stable online algorithm which should
   * be suitable for large numbers of records
   * similar to:
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   */
  val zero = AveragedValue(0L, 0.0)

  override def isNonZero(av: AveragedValue) = (av.count != 0L)

  override def negate(av: AveragedValue) = AveragedValue(-av.count, av.value)

  def plus(cntAve1: AveragedValue, cntAve2: AveragedValue): AveragedValue = {
    val (big, small) = if (cntAve1.count >= cntAve2.count)
      (cntAve1, cntAve2)
    else
      (cntAve2, cntAve1)
    val n = big.count
    val k = small.count
    val newCnt = n + k
    if (newCnt == n) {
      // Handle zero without allocation
      big
    } else if (newCnt == 0L) {
      zero
    } else {
      val an = big.value
      val ak = small.value
      val scaling = k.toDouble / newCnt
      // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
      val newAve = if (scaling < STABILITY_CONSTANT) (an + (ak - an) * scaling) else (n * an + k * ak) / newCnt
      new AveragedValue(newCnt, newAve)
    }
  }
}

object Averager extends MonoidAggregator[Double, Double] {
  type B = AveragedValue
  val monoid = AveragedGroup
  def prepare(value: Double) = AveragedValue(value)
  def present(average: AveragedValue) = average.value
}
