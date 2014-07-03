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
import com.twitter.util.{ Duration, Future, FuturePool }
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.{ Set => MSet, ListBuffer }
import scala.collection.breakOut

/**
 * @author Ian O Connell
 */

class AsyncListSum[Key, Value](bufferSize: BufferSize,
  override val flushFrequency: FlushFrequency,
  override val softMemoryFlush: MemoryFlushPercent,
  workPool: FuturePool)(implicit sg: Semigroup[Value])
  extends AsyncSummer[(Key, Value), Map[Key, Value]]
  with WithFlushConditions[(Key, Value), Map[Key, Value]] {

  require(bufferSize.v > 0, "Use the Null summer for an empty async summer")

  private case class MapContainer(privBuf: List[Value], size: Int) {
    def this(v: Value) = this(List[Value](v), 1)

    def addValue(v: Value): MapContainer = new MapContainer(v :: privBuf, size + 1)

    override def equals(o: Any) = o match {
      case that: MapContainer => that eq this
      case _ => false
    }

    lazy val toSeq = privBuf.reverse
  }

  protected override val emptyResult = Map.empty[Key, Value]
  private[this] final val queueMap = new ConcurrentHashMap[Key, MapContainer](bufferSize.v)
  private[this] final val elementsInCache = new AtomicInteger(0)

  override def isFlushed: Boolean = elementsInCache.get == 0

  override def flush: Future[Map[Key, Value]] =
    workPool {
      // Take a copy of the keyset into a scala set (toSet forces the copy)
      // We want this to be safe around uniqueness in the keys coming out of the keys.flatMap
      val keys = MSet[Key]()
      keys ++= queueMap.keySet.iterator.asScala

      keys.flatMap { k =>
        val retV = queueMap.remove(k)

        if (retV != null) {
          val newRemaining = elementsInCache.addAndGet(retV.size * -1)
          sg.sumOption(retV.toSeq).map(v => (k, v))
        } else None
      }(breakOut)
    }

  @annotation.tailrec
  private[this] final def doInsert(key: Key, value: Value) {
    val success = if (queueMap.containsKey(key)) {
      val oldValue = queueMap.get(key)
      if (oldValue != null) {
        queueMap.replace(key, oldValue, oldValue.addValue(value))
      } else {
        false // Removed between the check above and fetching
      }
    } else {
      // Test if something else has raced into our spot.
      queueMap.putIfAbsent(key, new MapContainer(value)) == null
    }

    if (success) {
      // Successful insert
      elementsInCache.addAndGet(1)
    } else {
      return doInsert(key, value)
    }
  }

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    vals.foreach {
      case (k, v) =>
        doInsert(k, v)
    }

    if (elementsInCache.get >= bufferSize.v) {
      flush
    } else {
      Future.value(Map.empty)
    }
  }
}