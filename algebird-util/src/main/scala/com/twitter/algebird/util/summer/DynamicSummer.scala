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
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

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

object DynamicSummer {
  // 1%
  val DEFAULT_HH_PERCENT = HeavyHittersPercent(0.01f)
  val DEFAULT_ROLL_OVER_FREQUENCY = RollOverFrequency(1000000L)
  val DEFAULT_UPDATE_FREQUENCY = UpdateFrequency(2)

  def apply[Key, Value](flushFrequency: FlushFrequency, softMemoryFlush: MemoryFlushPercent, backingSummer: AsyncSummer[(Key, Value), Iterable[(Key, Value)]]) =
    new DynamicSummer[Key, Value](DEFAULT_HH_PERCENT, DEFAULT_UPDATE_FREQUENCY, DEFAULT_ROLL_OVER_FREQUENCY, flushFrequency, softMemoryFlush, backingSummer)

  def apply[Key, Value](hhPct: HeavyHittersPercent, updateFreq: UpdateFrequency, roFreq: RollOverFrequency,
                        flushFrequency: FlushFrequency, softMemoryFlush: MemoryFlushPercent, backingSummer: AsyncSummer[(Key, Value), Iterable[(Key, Value)]]) =
    new DynamicSummer[Key, Value](hhPct, updateFreq, roFreq, flushFrequency, softMemoryFlush, backingSummer)

}

class DynamicSummer[K, V](hhPct: HeavyHittersPercent, updateFreq: UpdateFrequency, roFreq: RollOverFrequency, override val flushFrequency: FlushFrequency,
                              override val softMemoryFlush: MemoryFlushPercent,
                              backingSummer: AsyncSummer[(K, V), Iterable[(K, V)]])
                              extends AsyncSummer[(K, V), Iterable[(K, V)]]
                              with WithFlushConditions[(K, V), Iterable[(K, V)]] {

  type T = (K, V) // We only treat the K, V types as a pair almost exclusively in this class.

  override def flush: Future[Iterable[T]] = backingSummer.flush
  override def isFlushed = backingSummer.isFlushed
  override val emptyResult = Seq[T]()

  private[this] final val WIDTH = 1000
  private[this] final val DEPTH = 4
  private[this] final val hh = new java.util.HashMap[Long, Long]()
  private[this] final var totalCount = 0L
  private[this] final var hhMinReq = 0L
  private[this] final val hhPercent = hhPct.toFloat
  private[this] final val updateFrequency = updateFreq.toInt
  private[this] final val rollOverFrequency = roFreq.toLong
  private[this] final val countsTable = Array.fill(WIDTH * DEPTH)(0L)

  private[this] final val hashes: IndexedSeq[CMSHash] = {
    val r = new scala.util.Random(5)
    (0 until DEPTH).map { _ => CMSHash(r.nextInt, 0, WIDTH) }
  }.toIndexedSeq

  @inline
  private[this] final def pruneHH {
    val iter = hh.values.iterator
    while(iter.hasNext) {
      val n = iter.next
      if(n < hhMinReq) {
        iter.remove
      }
    }
  }

  @inline
  private[this] final def frequencyEst(item : Long): Long = {
    var min = Long.MaxValue
    var indx = 0
    while (indx < DEPTH) {
      val newVal = countsTable(indx*WIDTH + hashes(indx)(item))
      if(newVal < min) min = newVal
      indx += 1
    }
    min
  }


  // Update functions in the write path
  // a synchronized guard should be used around these
  // to ensure consistent updates to backing data structures
  @inline
  private[this] final def updateItem(item: Long) {
    totalCount += 1L
    hhMinReq = (hhPercent * totalCount).toLong
    var indx = 0
    while (indx < DEPTH) {
      val offset = indx*WIDTH + hashes(indx)(item)
      countsTable.update(offset, countsTable(offset) + 1L)
      indx += 1
    }
    updateHH(item)
  }

  @inline
  private[this] final def updateHH(item : Long) {
    if(hh.containsKey(item)) {
      val v = hh.get(item)
      val newItemCount =  + 1L
      if (newItemCount < hhMinReq) {
        pruneHH
      } else {
        hh.put(item, newItemCount)
      }
    } else {
      val newItemCount = frequencyEst(item) + 1L
      if (newItemCount >= hhMinReq) {
        hh.put(item, totalCount)
      }
      pruneHH
    }
  }

  // We include the ability to reset the CMS so we can age our counters
  // over time
  private[this] def resetCMS {
    hh.clear
    totalCount = 0L
    hhMinReq = 0L
    var indx = 0
    while(indx < WIDTH * DEPTH) {
      countsTable.update(indx, 0L)
      indx += 1
    }
  }
  // End of thread-unsafe update steps

  private[this] final val updateStep = new java.util.concurrent.atomic.AtomicLong(0L)

  @inline
  private[this] final def updateCMSTestMatch(t: Long): Boolean = {
    // This is the entry point from the iterator into our CMS implementation
    // We only update on certain steps < a threshold and on every nTh step.

    // We only acquire locks/synchronize when hitting the update/write path.
    // most passes into this function will just hit the final line(containsKey).
    // which is our thread safe read path.
    val newCounter = updateStep.incrementAndGet
    if (newCounter > rollOverFrequency) {
      hh.synchronized {
        updateStep.set(1L)
        resetCMS
      }
    }
    if(newCounter < 1000L || newCounter % updateFrequency == 0L) {
      hh.synchronized {
        updateItem(t)
      }
    }
    hh.containsKey(t)
  }

  private[this] final def wrapTraversableOnce(t: Iterator[T]): (ListBuffer[T], Iterator[T]) = {
    val store = new ListBuffer[T]
    val iter = new Iterator[T] {
      var head: T = null
      override def hasNext: Boolean = {
        if(head != null) return true
        while(t.hasNext) {
          val tmp = t.next
          if(updateCMSTestMatch(tmp._1.hashCode)) {
            head = tmp // its a heavy hitter
            return true
          } else {
            store += tmp
          }
        }
        false
      }
      def next = {
        if(hasNext) {
          val t = head
          head = null
          t
        } else {
          throw new Exception("Calling next on empty iterator")
        }
      }
    }
    (store, iter)
  }

  def addAll(vals: TraversableOnce[T]): Future[Iterable[T]] = {
    val (s, iter) = wrapTraversableOnce(vals.toIterator)
    if(iter.hasNext) {
      backingSummer.addAll(iter.toTraversable).map { fResp =>
        if(fResp.isEmpty) {
          s
        } else {
          fResp ++ s
        }
      }
    } else {
      Future.value(s)
    }
  }
}