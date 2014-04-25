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
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

/**
 * @author Ian O Connell
 */

class DynamicSummer[Key, Value](hhPct: Float, override val flushFrequency: FlushFrequency,
                              override val softMemoryFlush: MemoryFlushPercent,
                              backingSummer: AsyncSummer[(Key, Value), Iterable[(Key, Value)]])
                             (implicit sg: Semigroup[Value])
                              extends AsyncSummer[(Key, Value), Iterable[(Key, Value)]]
                              with WithFlushConditions[(Key, Value), Iterable[(Key, Value)]] {
  type T = (Key, Value)
  override def flush: Future[Iterable[(Key, Value)]] = backingSummer.flush
  override def isFlushed = backingSummer.isFlushed
  override val emptyResult = Seq[(Key, Value)]()


  private[this] final val hh = new java.util.HashMap[Long, Long]()
  private[this] final var totalCount = 0L
  private[this] final var hhMinReq = 0L
  private[this] final val HH_PCT = hhPct
  private[this] final val WIDTH = 1000
  private[this] final val DEPTH = 2
  private[this] final val countsTable = Array.fill(WIDTH * DEPTH)(0L)
  private[this] final val hashes: IndexedSeq[CMSHash] = {
    val r = new scala.util.Random(5)
    (0 until DEPTH).map { _ => CMSHash(r.nextInt, 0, WIDTH) }
  }.toIndexedSeq

  @inline
  def pruneHH {
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

  @inline
  private[this] final def updateItem(item: Long) {
    totalCount += 1L
    hhMinReq = (HH_PCT * totalCount).toLong
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

  @inline
  private def updateCMSTestMatch(t: Long): Boolean = hh.synchronized {
    updateItem(t)
    hh.containsKey(t)
  }

  def wrapTraversableOnce(t: Iterator[T]): (ArrayBuffer[T], Iterator[T]) = {
    val store = new ArrayBuffer[(Key, Value)]
    val iter = new Iterator[(Key, Value)] {
      var head: T = null
      override def hasNext: Boolean = {
        if(head != null) return true
        while(t.hasNext) {
          val tmp = t.next
          if(updateCMSTestMatch(tmp._1.hashCode)) {
            head = tmp
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

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Iterable[T]] = {
    val (s, iter) = wrapTraversableOnce(vals.toIterator)
    backingSummer.addAll(iter.toTraversable).map { fResp =>
      fResp ++ s
    }
  }
}