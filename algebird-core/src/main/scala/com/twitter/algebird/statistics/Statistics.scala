/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.algebird.statistics

import com.twitter.algebird.{Semigroup, Monoid, Group, Ring}

/**
 * These wrappers can be used to collect statistics around usage of monoids
 * Unlike most semigroups, monoids, etc they are not thread safe and one instance should be used per thread.
 * 
 * @author Julien Le Dem
 */

/**
 * internal collection of a distribution of values on a log scale
 */
private class Statistics {
  import scala.math.{pow, min}
  val maxPow = 10
  var distribution: Array[Long] = new Array(maxPow + 1)
  var total: Float = 0

  def put(v: Int) {
    total += v
    val log2 = 32 - Integer.numberOfLeadingZeros(v);
    distribution(min(log2, maxPow)) += 1
  }

  def count() = distribution.sum

  override def toString =
      distribution.zipWithIndex.map { case (v, i)  => (if(i == maxPow) ">" else "<" + pow(2, i).toInt) + ": " + v }.mkString(", ") +
      ", avg=" + total / count + " count=" + count

}

/** used to keep track of stats and time spent processing iterators passed to the methods */
private class IterCallStatistics {
  val countStats = new Statistics
  var totalCallTime: Float = 0

  /** used to count how many values are pulled from the Iterator without iterating twice */
  private class CountingIterator[T](val i: Iterator[T]) extends Iterator[T] {
    var nextCount: Int = 0
    override def hasNext = i.hasNext
    override def next = {
      val n = i.next
      nextCount += 1
      n
    }
  }

  /** measures the time spent calling f on iter and the size of iter */
  def measure[T, O](iter: TraversableOnce[T]) (f: (TraversableOnce[T]) => O): O = {
    val ci = new CountingIterator(iter.toIterator)
    val t0 = System.currentTimeMillis()
    val r = f(ci)
    val t1 = System.currentTimeMillis()
    countStats.put(ci.nextCount)
    totalCallTime += (t1 - t0)
    return r
  }

  override def toString =
    countStats.toString + ", " +
    "total time: " + totalCallTime + "ms, " +
    "avg time: " + (totalCallTime / countStats.count)
}

/** collect statistics about the calls to the wrapped Semigroup */
class StatisticsSemigroup[T] (implicit wrappedSemigroup: Semigroup[T])
  extends Semigroup[T] {

  private var plusCallsCount: Long = 0
  private val sumOptionCallsStats = new IterCallStatistics

  // access to collected stats
  def getPlusCallCount = plusCallsCount
  def getSumOptionCallCount = sumOptionCallsStats.countStats.count
  def getSumOptionCallTime = sumOptionCallsStats.totalCallTime

  override def plus(x: T, y: T) = {
    plusCallsCount += 1
    Semigroup.plus(x, y)
  }

  override def sumOption(iter: TraversableOnce[T]): Option[T] =
    sumOptionCallsStats.measure(iter) { Semigroup.sumOption(_) }

  override def toString =
    "plus calls: " + plusCallsCount + "\n" +
    "sumOption calls: " + sumOptionCallsStats
}

/**
 * @see StatisticsSemigroup
 */
class StatisticsMonoid[T] (implicit wrappedMonoid: Monoid[T])
  extends StatisticsSemigroup[T] with Monoid[T] {

  private var zeroCallsCount: Long = 0
  private val sumCallsStats = new IterCallStatistics

  // access to collected stats
  def getZeroCallCount = zeroCallsCount
  def getSumCallCount = sumCallsStats.countStats.count
  def getSumCallTime = sumCallsStats.totalCallTime

  override def zero = {
    zeroCallsCount += 1
    Monoid.zero
  }

  override def sum(vs: TraversableOnce[T]): T =
    sumCallsStats.measure(vs) { Monoid.sum(_) }

  override def toString =
    super.toString + "\n" +
    "zero calls: " + zeroCallsCount + "\n" +
    "sum calls: " + sumCallsStats
}

/**
 * @see StatisticsSemigroup
 */
class StatisticsGroup[T](implicit group: Group[T])
  extends StatisticsMonoid[T] with Group[T] {

  private var negateCallsCount: Long = 0
  private var minusCallsCount: Long = 0

  // access to collected stats
  def getNegateCallCount = negateCallsCount
  def getMinusCallCount = minusCallsCount

  override def negate(x: T) = {
    negateCallsCount += 1
    Group.negate(x)
  }

  override def minus(l : T, r : T) = {
    minusCallsCount += 1
    Group.minus(l, r)
  }

  override def toString =
    super.toString + "\n" +
    "negate calls: " + negateCallsCount + "\n" +
    "minus calls: " + minusCallsCount
}

/**
 * @see StatisticsSemigroup
 */
class StatisticsRing[T] (implicit ring: Ring[T])
  extends StatisticsGroup[T] with Ring[T] {

  private var oneCallsCount: Long = 0
  private var timesCallsCount: Long = 0
  private val productCallsStats = new IterCallStatistics

  // access to collected stats
  def getOneCallCount = oneCallsCount
  def getTimesCallCount = timesCallsCount
  def getProductCallCount = productCallsStats.countStats.count
  def getProductCallTime = productCallsStats.totalCallTime

  override def one = {
    oneCallsCount += 1
    Ring.one
  }

  override def times(x: T, y: T) = {
    timesCallsCount += 1
    Ring.times(x, y)
  }

  override def product(iter : TraversableOnce[T]): T =
    productCallsStats.measure(iter) { Ring.product(_) }

  override def toString =
    super.toString + "\n" +
    "one calls: " + oneCallsCount + "\n" +
    "time calls: " + timesCallsCount + "\n" +
    "product calls: " + productCallsStats
}

