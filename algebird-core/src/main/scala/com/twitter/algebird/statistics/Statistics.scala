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

import com.twitter.algebird.{ Semigroup, Monoid, Group, Ring }

/**
 * These wrappers can be used to collect statistics around usage of monoids
 * They are thread safe unless false is passed to the constructor (to remove overhead when threads are not used)
 *
 * @author Julien Le Dem
 */

/** collect statistics about the calls to the wrapped Semigroup */
class StatisticsSemigroup[T](threadSafe: Boolean = true)(implicit wrappedSemigroup: Semigroup[T])
    extends Semigroup[T] {

  private[this] final val plusCallsCount = Counter(threadSafe)
  private[this] final val sumOptionCallsStats = new IterCallStatistics(threadSafe)

  // access to collected stats
  def getPlusCallCount: Long = plusCallsCount.get
  def getSumOptionCallCount: Long = sumOptionCallsStats.getCallCount
  def getSumOptionCallTime: Long = sumOptionCallsStats.getTotalCallTime

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
class StatisticsMonoid[T](threadSafe: Boolean = true)(implicit wrappedMonoid: Monoid[T])
    extends StatisticsSemigroup[T](threadSafe) with Monoid[T] {

  private[this] final val zeroCallsCount = Counter(threadSafe)
  private[this] final val sumCallsStats = new IterCallStatistics(threadSafe)

  // access to collected stats
  def getZeroCallCount: Long = zeroCallsCount.get
  def getSumCallCount: Long = sumCallsStats.getCallCount
  def getSumCallTime: Long = sumCallsStats.getTotalCallTime

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
class StatisticsGroup[T](threadSafe: Boolean = true)(implicit group: Group[T])
    extends StatisticsMonoid[T](threadSafe) with Group[T] {

  private[this] final val negateCallsCount = Counter(threadSafe)
  private[this] final val minusCallsCount = Counter(threadSafe)

  // access to collected stats
  def getNegateCallCount: Long = negateCallsCount.get
  def getMinusCallCount: Long = minusCallsCount.get

  override def negate(x: T) = {
    negateCallsCount.increment
    Group.negate(x)
  }

  override def minus(l: T, r: T) = {
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
class StatisticsRing[T](threadSafe: Boolean = true)(implicit ring: Ring[T])
    extends StatisticsGroup[T](threadSafe) with Ring[T] {

  private[this] final val oneCallsCount = Counter(threadSafe)
  private[this] final val timesCallsCount = Counter(threadSafe)
  private[this] final val productCallsStats = new IterCallStatistics(threadSafe)

  // access to collected stats
  def getOneCallCount: Long = oneCallsCount.get
  def getTimesCallCount: Long = timesCallsCount.get
  def getProductCallCount: Long = productCallsStats.getCallCount
  def getProductCallTime: Long = productCallsStats.getTotalCallTime

  override def one = {
    oneCallsCount.increment
    Ring.one
  }

  override def times(x: T, y: T) = {
    timesCallsCount.increment
    Ring.times(x, y)
  }

  override def product(iter: TraversableOnce[T]): T =
    productCallsStats.measure(iter) { Ring.product(_) }

  override def toString =
    super.toString + "\n" +
      "one calls: " + oneCallsCount + "\n" +
      "time calls: " + timesCallsCount + "\n" +
      "product calls: " + productCallsStats
}

