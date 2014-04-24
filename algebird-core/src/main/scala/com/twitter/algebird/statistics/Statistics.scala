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
import java.util.concurrent.atomic.AtomicLong

/**
 * These wrappers can be used to collect statistics around usage of monoids
 * They are thread safe unless false is passed to the constructor (to remove overhead when threads are not used)
 * 
 * @author Julien Le Dem
 */

/** deals with optionally making this thread safe */
private object Counter {
  def apply(threadSafe: Boolean): Counter = if (threadSafe) AtomicCounter() else PlainCounter()
}

private sealed trait Counter {
  def increment: Unit
  def add(v: Long): Unit
  def get: Long
  def toDouble = get.toDouble
  override def toString = get.toString
}

private object AtomicCounter {
  def apply(): Counter = new AtomicCounter
}

/** thread safe */
private class AtomicCounter extends Counter {
  val counter = new AtomicLong(0)
  override def increment = counter.incrementAndGet
  override def add(v: Long) = counter.addAndGet(v)
  override def get = counter.get
}

private object PlainCounter {
  def apply(): Counter = new PlainCounter
}

/** not thread safe */
private class PlainCounter extends Counter {
  var counter: Long = 0
  override def increment = counter += 1
  override def add(v: Long) = counter += v
  override def get = counter
}

/**
 * internal collection of a distribution of values on a log scale
 */
private class Statistics(threadSafe: Boolean) {
  import scala.math.min
  import java.lang.Long.numberOfLeadingZeros
  val maxBucket = 10
  val distribution: Array[Counter] = new Array(maxBucket + 1)
  for (i <- 0 to maxBucket) distribution(i) = Counter(threadSafe)
  val total = Counter(threadSafe)

  def put(v: Long) {
    total.add(v)
    // log2(v + 1) for v up to 2^maxBucket
    val bucket = min(64 - numberOfLeadingZeros(v), maxBucket)
    distribution(bucket).increment
  }

  def count() = distribution.foldLeft(0L) { _ + _.get } // sum

  def pow2(i: Int): Int = 1 << i

  override def toString =
      distribution.zipWithIndex.map { case (v, i)  => (if(i == maxBucket) ">" else "<" + pow2(i)) + ": " + v }.mkString(", ") +
      ", avg=" + total.toDouble / count + " count=" + count

}

/** used to keep track of stats and time spent processing iterators passed to the methods */
private class IterCallStatistics(threadSafe: Boolean) {
  val countStats = new Statistics(threadSafe)
  val totalCallTime = Counter(threadSafe)

  /** used to count how many values are pulled from the Iterator without iterating twice */
  private class CountingIterator[T](val i: Iterator[T]) extends Iterator[T] {
    var nextCount: Long = 0
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
    totalCallTime.add(t1 - t0)
    return r
  }

  override def toString =
    countStats.toString + ", " +
    "total time: " + totalCallTime + "ms, " +
    "avg time: " + (totalCallTime.toDouble / countStats.count)
}

/** collect statistics about the calls to the wrapped Semigroup */
class StatisticsSemigroup[T](threadSafe: Boolean = true) (implicit wrappedSemigroup: Semigroup[T])
  extends Semigroup[T] {

  private val plusCallsCount = Counter(threadSafe)
  private val sumOptionCallsStats = new IterCallStatistics(threadSafe)

  // access to collected stats
  def getPlusCallCount: Long = plusCallsCount.get
  def getSumOptionCallCount: Long = sumOptionCallsStats.countStats.count
  def getSumOptionCallTime: Long = sumOptionCallsStats.totalCallTime.get

  override def plus(x: T, y: T) = {
    plusCallsCount.increment
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
class StatisticsMonoid[T](threadSafe: Boolean = true) (implicit wrappedMonoid: Monoid[T])
  extends StatisticsSemigroup[T](threadSafe) with Monoid[T] {

  private val zeroCallsCount = Counter(threadSafe)
  private val sumCallsStats = new IterCallStatistics(threadSafe)

  // access to collected stats
  def getZeroCallCount: Long = zeroCallsCount.get
  def getSumCallCount: Long = sumCallsStats.countStats.count
  def getSumCallTime: Long = sumCallsStats.totalCallTime.get

  override def zero = {
    zeroCallsCount.increment
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
class StatisticsGroup[T](threadSafe: Boolean = true) (implicit group: Group[T])
  extends StatisticsMonoid[T](threadSafe) with Group[T] {

  private val negateCallsCount = Counter(threadSafe)
  private val minusCallsCount = Counter(threadSafe)

  // access to collected stats
  def getNegateCallCount: Long = negateCallsCount.get
  def getMinusCallCount: Long = minusCallsCount.get

  override def negate(x: T) = {
    negateCallsCount.increment
    Group.negate(x)
  }

  override def minus(l : T, r : T) = {
    minusCallsCount.increment
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
class StatisticsRing[T](threadSafe: Boolean = true) (implicit ring: Ring[T])
  extends StatisticsGroup[T](threadSafe) with Ring[T] {

  private val oneCallsCount = Counter(threadSafe)
  private val timesCallsCount = Counter(threadSafe)
  private val productCallsStats = new IterCallStatistics(threadSafe)

  // access to collected stats
  def getOneCallCount: Long = oneCallsCount.get
  def getTimesCallCount: Long = timesCallsCount.get
  def getProductCallCount: Long = productCallsStats.countStats.count
  def getProductCallTime: Long = productCallsStats.totalCallTime.get

  override def one = {
    oneCallsCount.increment
    Ring.one
  }

  override def times(x: T, y: T) = {
    timesCallsCount.increment
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

