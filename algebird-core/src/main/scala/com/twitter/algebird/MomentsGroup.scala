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
 * A class to calculate the first five central moments over a sequence of Doubles.
 * Given the first five central moments, we can then calculate metrics like skewness
 * and kurtosis.
 *
 * m{i} denotes the ith central moment.
 */
case class Moments(m0 : Long, m1 : Double, m2 : Double, m3 : Double, m4 : Double) {  
  def count = m0
  
  def mean = m1
  
  // Population variance, not sample variance.
  def variance = m2 / count
  
  // Population standard deviation, not sample standard deviation.
  def stddev = math.sqrt(variance)
  
  def skewness = math.sqrt(count) * m3 / math.pow(m2, 1.5)
  
  def kurtosis = count * m4 / math.pow(m2, 2) - 3
}

object Moments {
  implicit val group = MomentsGroup
  implicit val aggregator = MomentsAggregator

  // Create a Moments object given a single value. This is useful for
  // initializing moment calculations at the start of a stream.
  def apply[V <% Double](value : V) = new Moments(1L, value, 0, 0, 0)
  
  def apply[V <% Double](m0 : Long, m1 : V, m2 : V, m3 : V, m4 : V) = 
    new Moments(m0, m1, m2, m3, m4)
}

/**
 * A monoid to perform moment calculations.
 */
object MomentsGroup extends Group[Moments] {

  // When combining averages, if the counts sizes are too close we should use a different
  // algorithm. This constant defines how close the ratio of the smaller to the total count
  // can be.
  private val STABILITY_CONSTANT = 0.1

  val zero = Moments(0L, 0.0, 0.0, 0.0, 0.0)

  override def negate(a : Moments) : Moments = {
    Moments(-a.count, a.m1, -a.m2, -a.m3, -a.m4)
  }
  
  // Combines the moment calculations from two streams.
  // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
  // for more information on the formulas used to update the moments.
  def plus(a : Moments, b : Moments) : Moments = {
    val delta = b.mean - a.mean
    val countCombined = a.count + b.count
    if(countCombined == 0)
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

  /**
   * Given two streams of doubles A and B, with the specified counts and means,
   * calculate the mean of the combined stream.
   */
  def getCombinedMean(countA: Long, meanA: Double, countB: Long, meanB: Double): Double = {
    val (big, small) =
      if (math.abs(countA) >= math.abs(countB))
        ((countA, meanA), (countB, meanB))
      else
        ((countB, meanB), (countA, meanA))

    val (countBig, meanBig) = big
    val (countSmall, meanSmall) = small
    
    if (countSmall == 0) {
      meanBig
    } else {
      val countCombined = countSmall + countBig
      val scaling = countSmall.toDouble / countCombined
      val meanCombined =
        if (math.abs(scaling) < STABILITY_CONSTANT)
          // This formula for the combined mean is only stable if
          // countA is not approximately countB.
          meanBig + (meanSmall - meanBig) * scaling
        else
          // Use this more stable formulation if the sizes of the two streams
          // are close.
          (countBig * meanBig + countSmall * meanSmall) / countCombined
          
      meanCombined
    }
  }
}

object MomentsAggregator extends MonoidAggregator[Double, Moments, Moments] {
  val monoid = MomentsGroup

  def prepare(input: Double): Moments = Moments(input)
  def present(m: Moments) = m
}
