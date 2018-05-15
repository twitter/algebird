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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.algebird._
import com.twitter.util.{Future, FuturePool}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Set => MSet}
import com.twitter.algebird.util.UtilAlgebras._

/**
 * @author Ian O Connell
 */
class AsyncListSum[Key, Value](bufferSize: BufferSize,
                               override val flushFrequency: FlushFrequency,
                               override val softMemoryFlush: MemoryFlushPercent,
                               override val memoryIncr: Incrementor,
                               override val timeoutIncr: Incrementor,
                               insertOp: Incrementor,
                               insertFails: Incrementor,
                               sizeIncr: Incrementor,
                               tuplesIn: Incrementor,
                               tuplesOut: Incrementor,
                               workPool: FuturePool,
                               compact: Compact,
                               compatSize: CompactionSize)(implicit sg: Semigroup[Value])
    extends AsyncSummer[(Key, Value), Map[Key, Value]]
    with WithFlushConditions[(Key, Value), Map[Key, Value]] {

  require(bufferSize.v > 0, "Use the Null summer for an empty async summer")

  private case class MapContainer(privBuf: List[Future[Value]], size: Int, compact: Compact) {
    def this(v: Value, compact: Compact) =
      this(List[Future[Value]](Future.value(v)), 1, compact)

    def addValue(v: Value): (MapContainer, Int) =
      if (compact.flag && size > compatSizeInt) {
        val newV = workPool {
          fSg.sumOption(Future.value(v) :: privBuf).get
        }.flatten
        (new MapContainer(List(newV), 1, compact), (size - 1) * -1)
      } else {
        (new MapContainer(Future.value(v) :: privBuf, size + 1, compact), 1)
      }

    override def equals(o: Any) = o match {
      case that: MapContainer => that eq this
      case _                  => false
    }

    lazy val toSeq = privBuf.reverse
  }

  protected override val emptyResult = Map.empty[Key, Value]
  private[this] final val queueMap =
    new ConcurrentHashMap[Key, MapContainer](bufferSize.v)
  private[this] final val elementsInCache = new AtomicInteger(0)
  val fSg: Semigroup[Future[Value]] = implicitly[Semigroup[Future[Value]]]
  private[this] val innerBuffSize = bufferSize.v
  private[this] val compatSizeInt = compatSize.toInt

  override def isFlushed: Boolean = elementsInCache.get == 0

  override def flush: Future[Map[Key, Value]] =
    workPool {
      // Take a copy of the keyset into a scala set (toSet forces the copy)
      // We want this to be safe around uniqueness in the keys coming out of the keys.flatMap
      val keys = MSet[Key]()
      keys ++= queueMap.keySet.iterator.asScala

      val lFuts = Future.collect(keys.toIterator.flatMap { k =>
        val retV = queueMap.remove(k)

        if (retV != null) {
          val newRemaining = elementsInCache.addAndGet(retV.size * -1)
          fSg.sumOption(retV.toSeq).map(v => v.map((k, _)))
        } else None
      }.toSeq)
      lFuts
        .map(_.toMap)
        .foreach { r =>
          tuplesOut.incrBy(r.size)
        }
    }.flatten

  @annotation.tailrec
  private[this] final def doInsert(key: Key, value: Value) {
    tuplesIn.incr
    val (success, countChange) = if (queueMap.containsKey(key)) {
      val oldValue = queueMap.get(key)
      if (oldValue != null) {
        val (newValue, countChange) = oldValue.addValue(value)
        (queueMap.replace(key, oldValue, newValue), countChange)
      } else {
        (false, 0) // Removed between the check above and fetching
      }
    } else {
      // Test if something else has raced into our spot.
      (queueMap.putIfAbsent(key, new MapContainer(value, compact)) == null, 1)
    }

    if (success) {
      // Successful insert
      elementsInCache.addAndGet(countChange)
    } else {
      insertFails.incr
      return doInsert(key, value)
    }
  }

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] =
    workPool {
      insertOp.incr
      vals.foreach {
        case (k, v) =>
          doInsert(k, v)
      }

      if (elementsInCache.get >= innerBuffSize) {
        sizeIncr.incr
        flush
      } else {
        Future.value(Map.empty[Key, Value])
      }
    }.flatten
}

case class CompactionSize(toInt: Int) extends AnyVal
case class Compact(flag: Boolean) extends AnyVal
