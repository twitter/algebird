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
package com.twitter.algebird.util.summer

import com.twitter.algebird._
import com.twitter.util.{Duration, Future, FuturePool}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import scala.collection.mutable.ListBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.{Set => MSet, ListBuffer}

/**
 * @author Ian O Connell
 *
 * This class is designed to use a local mutable CMS to skip keeping low freqeuncy keys in a buffer.
 */
// The update frequency is how often we should update the mutable CMS
// other steps will just query the pre-established HH's
// This will only kick in after the first 1000 tuples since a Roll Over
case class UpdateFrequency(toInt: Int)

// This is after how many entries we will reset the CMS
// This is to account for temporal changes in the HH's
case class RollOverFrequency(toLong: Long)

// The heavy hitters percent is used to control above what % of items we should send to the backing
// aggregator
case class HeavyHittersPercent(toFloat: Float)

class ApproxHHTracker(hhPct: HeavyHittersPercent, updateFreq: UpdateFrequency, roFreq: RollOverFrequency) {

  import CMSHasherImplicits._

  private[this] final val WIDTH = 1000
  private[this] final val DEPTH = 4
  private[this] final val hh = new java.util.HashMap[Int, Long]()
  private[this] final var totalCount = 0L
  private[this] final var hhMinReq = 0L
  private[this] final val hhPercent = hhPct.toFloat
  private[this] final val updateFrequency = updateFreq.toInt
  private[this] final val rollOverFrequency = roFreq.toLong
  private[this] final var countsTable = Array.fill(WIDTH * DEPTH)(0L)

  private[this] final val hashes: IndexedSeq[CMSHash[Long]] = {
    val r = new scala.util.Random(5)
    (0 until DEPTH).map { _ =>
      CMSHash[Long](r.nextInt, 0, WIDTH)
    }
  }.toIndexedSeq

  @inline
  private[this] final def frequencyEst(item: Long): Long = {
    var min = Long.MaxValue
    var indx = 0
    while (indx < DEPTH) {
      val newVal = countsTable(indx * WIDTH + hashes(indx)(item))
      if (newVal < min) min = newVal
      indx += 1
    }
    min
  }

  // Update functions in the write path
  // a synchronized guard should be used around these
  // to ensure consistent updates to backing data structures
  @inline
  private[this] final def updateItem(item: Int) {
    totalCount += 1L
    hhMinReq = (hhPercent * totalCount).toLong
    var indx = 0
    while (indx < DEPTH) {
      val offset = indx * WIDTH + hashes(indx)(item)
      countsTable.update(offset, countsTable(offset) + 1L)
      indx += 1
    }

    updateHH(item)
  }

  @inline
  private[this] final def updateHH(item: Int) {
    @inline
    def pruneHH(): Unit = {
      val iter = hh.values.iterator
      while (iter.hasNext) {
        val n = iter.next
        if (n < hhMinReq) {
          iter.remove
        }
      }
    }

    if (hh.containsKey(item)) {
      val v = hh.get(item)
      val newItemCount = +1L
      if (newItemCount < hhMinReq) {
        pruneHH()
      } else {
        hh.put(item, newItemCount)
      }
    } else {
      val newItemCount = frequencyEst(item) + 1L
      if (newItemCount >= hhMinReq) {
        hh.put(item, totalCount)
      }
      pruneHH()
    }
  }

  // We include the ability to reset the CMS so we can age our counters
  // over time
  private[this] def resetCMS(): Unit = {
    hh.clear
    totalCount = 0L
    hhMinReq = 0L
    countsTable = Array.fill(WIDTH * DEPTH)(0L)
  }
  // End of thread-unsafe update steps

  private[this] final val updateStep =
    new java.util.concurrent.atomic.AtomicLong(0L)

  final def hhFilter(t: Int): Boolean = {
    // This is the entry point from the iterator into our CMS implementation
    // We only update on certain steps < a threshold and on every nTh step.

    // We only acquire locks/synchronize when hitting the update/write path.
    // most passes into this function will just hit the final line(containsKey).
    // which is our thread safe read path.
    val newCounter = updateStep.incrementAndGet
    if (newCounter > rollOverFrequency) {
      hh.synchronized {
        updateStep.set(1L)
        resetCMS()
      }
    }
    if (newCounter < 1000L || newCounter % updateFrequency == 0L) {
      hh.synchronized {
        updateItem(t)
      }
    }
    hh.containsKey(t)
  }

  // Tuples returned are (hh, non-HH)
  final def splitTraversableOnce[T](t: TraversableOnce[T],
                                    extractor: T => Int): (ListBuffer[T], ListBuffer[T]) = {
    val hh = new ListBuffer[T]
    val nonHH = new ListBuffer[T]

    t.foreach { t =>
      if (hhFilter(extractor(t)))
        hh += t
      else
        nonHH += t
    }

    (hh, nonHH)
  }
}

object HeavyHittersCachingSummer {
  val DEFAULT_HH_PERCENT = HeavyHittersPercent(0.01f)
  val DEFAULT_ROLL_OVER_FREQUENCY = RollOverFrequency(1000000L)
  val DEFAULT_UPDATE_FREQUENCY = UpdateFrequency(2)

  def apply[Key, Value](flushFrequency: FlushFrequency,
                        softMemoryFlush: MemoryFlushPercent,
                        memoryIncr: Incrementor,
                        timeoutIncr: Incrementor,
                        tuplesOut: Incrementor,
                        insertOp: Incrementor,
                        sizeIncr: Incrementor,
                        backingSummer: AsyncSummer[(Key, Value), Iterable[(Key, Value)]]) =
    new HeavyHittersCachingSummer[Key, Value](
      DEFAULT_HH_PERCENT,
      DEFAULT_UPDATE_FREQUENCY,
      DEFAULT_ROLL_OVER_FREQUENCY,
      flushFrequency,
      softMemoryFlush,
      memoryIncr,
      timeoutIncr,
      insertOp,
      backingSummer)

  def apply[Key, Value](hhPct: HeavyHittersPercent,
                        updateFreq: UpdateFrequency,
                        roFreq: RollOverFrequency,
                        flushFrequency: FlushFrequency,
                        softMemoryFlush: MemoryFlushPercent,
                        memoryIncr: Incrementor,
                        timeoutIncr: Incrementor,
                        tuplesOut: Incrementor,
                        insertOp: Incrementor,
                        sizeIncr: Incrementor,
                        backingSummer: AsyncSummer[(Key, Value), Iterable[(Key, Value)]]) =
    new HeavyHittersCachingSummer[Key, Value](
      hhPct,
      updateFreq,
      roFreq,
      flushFrequency,
      softMemoryFlush,
      memoryIncr,
      timeoutIncr,
      insertOp,
      backingSummer)

}

class HeavyHittersCachingSummer[K, V](hhPct: HeavyHittersPercent,
                                      updateFreq: UpdateFrequency,
                                      roFreq: RollOverFrequency,
                                      override val flushFrequency: FlushFrequency,
                                      override val softMemoryFlush: MemoryFlushPercent,
                                      override val memoryIncr: Incrementor,
                                      override val timeoutIncr: Incrementor,
                                      insertOp: Incrementor,
                                      backingSummer: AsyncSummer[(K, V), Iterable[(K, V)]])
    extends AsyncSummer[(K, V), Iterable[(K, V)]]
    with WithFlushConditions[(K, V), Iterable[(K, V)]] {

  type T = (K, V) // We only treat the K, V types as a pair almost exclusively in this class.

  override def flush: Future[Iterable[T]] = backingSummer.flush
  override def isFlushed = backingSummer.isFlushed
  override val emptyResult = Seq[T]()

  private[this] final val approxHH =
    new ApproxHHTracker(hhPct, updateFreq, roFreq)

  def addAll(vals: TraversableOnce[T]): Future[Iterable[T]] = {
    //todo not sure if need to increment as backing summer may already be doing it
    insertOp.incr
    val (hh, nonHH) = approxHH.splitTraversableOnce(vals, { t: T =>
      t._1.hashCode
    })

    if (!hh.isEmpty) {
      backingSummer.addAll(hh).map { fResp =>
        if (fResp.isEmpty) {
          nonHH
        } else {
          fResp.view ++ nonHH
        }
      }
    } else {
      Future.value(nonHH)
    }
  }
}
