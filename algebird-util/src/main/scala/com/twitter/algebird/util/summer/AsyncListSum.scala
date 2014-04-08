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
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.{Set => MSet}
/**
 * @author Ian O Connell
 */

class AsyncListSum[Key, Value](bufferSize: BufferSize,
                                          override val flushFrequency: FlushFrequency,
                                          override val softMemoryFlush: MemoryFlushPercent,
                                          workPool: FuturePool)
                                         (implicit semigroup: Semigroup[Value])
                                          extends AsyncSummer[(Key, Value), Map[Key, Value]]
                                          with WithFlushConditions[(Key, Value), Map[Key, Value]] {

  require(bufferSize.v > 0, "Use the Null summer for an empty async summer")

  protected override val emptyResult = Map.empty[Key, Value]

  private[this] final val queueMap = new ConcurrentHashMap[Key, IndexedSeq[Value]]()
  private[this] final val elementsInCache = new AtomicInteger(0)

  override def isFlushed: Boolean = elementsInCache.get == 0

  override def flush: Future[Map[Key, Value]] =
    workPool {
      didFlush // bumps timeout on the flush conditions
      // Take a copy of the keyset into a scala set (toSet forces the copy)
      // We want this to be safe around uniqueness in the keys coming out of the keys.flatMap
      val keys = queueMap.keySet.asScala.toSet
      keys.flatMap { k =>
        val retV = queueMap.remove(k)
        if(retV != null) {
          val newRemaining = elementsInCache.addAndGet(retV.size * -1)
          Semigroup.sumOption(retV).map(v => (k, v))
        }
        else None
      }.toMap
    }

  @annotation.tailrec
  private[this] final def doInsert(key: Key, vals: IndexedSeq[Value]) {
    require(key != null, "Key can not be null")
    val success = if (queueMap.containsKey(key)) {
      val oldValue = queueMap.get(key)
      if(oldValue != null) {
        val newValue = vals ++ oldValue
        queueMap.replace(key, oldValue, newValue)
      } else {
        false // Removed between the check above and fetching
      }
    } else {
      // Test if something else has raced into our spot.
      queueMap.putIfAbsent(key, vals) == null
    }

    if(success) {
      // Successful insert
      elementsInCache.addAndGet(vals.size)
    } else {
      return doInsert(key, vals)
    }
  }

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    val prepVals = vals.map { case (k, v) =>
      require(k != null, "Inserting a null key?")
      (k -> IndexedSeq(v))
    } : TraversableOnce[(Key, IndexedSeq[Value])]

    val curData = MapAlgebra.sumByKey(prepVals)

    curData.foreach { case (k, v) =>
      doInsert(k, v)
    }

    if(elementsInCache.get >= bufferSize.v) {
      flush
    } else {
      Future.value(Map.empty)
    }
  }
}