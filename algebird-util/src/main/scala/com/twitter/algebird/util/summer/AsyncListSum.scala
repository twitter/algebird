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

/**
 * @author Ian O Connell
 */

class AsyncListSum[Key, Value](cacheSize: Int,
                                          override val flushFrequency: Duration,
                                          override val softMemoryFlush: Float,
                                          workPool: FuturePool)
                                         (implicit semigroup: Semigroup[Value])
                                          extends AsyncSummer[Key, Value]
                                          with WithFlushConditions[Key, Value] {

  require(cacheSize > 0, "Use the Null summer for an empty async summer")

  private[this] final val queueMap = new ConcurrentHashMap[Key, IndexedSeq[Value]]
  private[this] final val elementsInCache = new AtomicInteger(0)

  override def forceTick: Future[Map[Key, Value]] =
    workPool {
      didFlush // bumps timeout on the flush conditions
      // Take a copy of the keyset into a scala set (toSet forces the copy)
      // We want this to be safe around uniqueness in the keys coming out of the keys.flatMap
      val keys = queueMap.keySet.asScala.toSet
      keys.flatMap { k =>
        val retV = queueMap.remove(k)
        if(retV != null) {
          elementsInCache.addAndGet(retV.size * -1)
          Semigroup.sumOption(retV).map(v => (k, v))
        }
        else None
      }.toMap
    }

  @annotation.tailrec
  private[this] final def doInsert(key: Key, vals: IndexedSeq[Value]) {
    val success = if (queueMap.containsKey(key)) {
      val oldValue = queueMap.get(key)
      val newValue = vals ++ oldValue
      queueMap.replace(key, oldValue, newValue)
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

  def insert(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    val prepVals = vals.map { case (k, v) =>
      Map(k -> IndexedSeq(v))
    } : TraversableOnce[Map[Key, IndexedSeq[Value]]]

    val curData = Semigroup.sumOption(prepVals).getOrElse(Map.empty)

    curData.foreach { case (k, v) =>
      doInsert(k, v)
    }

    if(elementsInCache.get >= cacheSize) {
      forceTick
    } else {
      Future.value(Map.empty)
    }
  }
}