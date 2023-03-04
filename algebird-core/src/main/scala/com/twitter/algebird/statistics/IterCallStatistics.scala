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

/**
 * used to keep track of stats and time spent processing iterators passed to the methods
 * @author
 *   Julien Le Dem
 */
private class IterCallStatistics(threadSafe: Boolean) {

  /**
   * internal collection of a distribution of values on a log scale
   */
  private class Statistics(threadSafe: Boolean) {
    import scala.math.min
    import java.lang.Long.numberOfLeadingZeros
    val maxBucket: Int = 10
    val distribution: IndexedSeq[Counter] = IndexedSeq.fill(maxBucket + 1)(Counter(threadSafe))
    val total: Counter = Counter(threadSafe)

    def put(v: Long): Unit = {
      total.add(v)
      // log2(v + 1) for v up to 2^maxBucket
      val bucket = min(64 - numberOfLeadingZeros(v), maxBucket)
      distribution(bucket).increment()
    }

    def count: Long = distribution.foldLeft(0L)(_ + _.get) // sum

    def pow2(i: Int): Int = 1 << i

    override def toString: String =
      distribution.zipWithIndex
        .map { case (v, i) =>
          (if (i == maxBucket) ">" else "<" + pow2(i)) + ": " + v
        }
        .mkString(", ") + ", avg=" + total.toDouble / count + " count=" + count

  }

  private[this] final val countStats = new Statistics(threadSafe)
  private[this] final val totalCallTime = Counter(threadSafe)

  /** used to count how many values are pulled from the Iterator without iterating twice */
  private class CountingIterator[T](val i: Iterator[T]) extends Iterator[T] {
    private[this] final var nextCount: Long = 0
    override def hasNext: Boolean = i.hasNext
    override def next(): T = {
      val n = i.next()
      nextCount += 1
      n
    }
    def getNextCount: Long = nextCount
  }

  /** measures the time spent calling f on iter and the size of iter */
  def measure[T, O](iter: TraversableOnce[T])(f: (TraversableOnce[T]) => O): O = {
    val ci = new CountingIterator(iter.toIterator)
    val t0 = System.currentTimeMillis()
    val r = f(ci)
    val t1 = System.currentTimeMillis()
    countStats.put(ci.getNextCount)
    totalCallTime.add(t1 - t0)
    r
  }

  def getCallCount: Long = countStats.count
  def getTotalCallTime: Long = totalCallTime.get

  override def toString: String =
    countStats.toString + ", " +
      "total time: " + totalCallTime + "ms, " +
      "avg time: " + (totalCallTime.toDouble / countStats.count)
}
