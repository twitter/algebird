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
import com.twitter.util.{Future, FuturePool}
import scala.collection.mutable.{ListBuffer, Map => MMap}
import scala.collection.breakOut

/**
 * @author Ian O Connell
 *
 * This is a simple asyncronous summer, where a shared mutable map is used between all readers/writers.
 * When flushing it acquires the lock, drains the mutable map but does the compaction without holding the lock.
 */
class AsyncListMMapSum[Key, Value](bufferSize: BufferSize,
                                   override val flushFrequency: FlushFrequency,
                                   override val softMemoryFlush: MemoryFlushPercent,
                                   override val memoryIncr: Incrementor,
                                   override val timeoutIncr: Incrementor,
                                   tuplesOut: Incrementor,
                                   insertOp: Incrementor,
                                   sizeIncr: Incrementor,
                                   workPool: FuturePool)(implicit sg: Semigroup[Value])
    extends AsyncSummer[(Key, Value), Map[Key, Value]]
    with WithFlushConditions[(Key, Value), Map[Key, Value]] {
  require(bufferSize.v > 0, "Use the Null summer for an empty async summer")

  private[this] final val queueMap = MMap[Key, ListBuffer[Value]]()
  private[this] final val mutex = new Object()
  @volatile private[this] var presentTuples = 0

  protected override val emptyResult = Map.empty[Key, Value]

  override def isFlushed: Boolean = mutex.synchronized { presentTuples == 0 }

  override def flush: Future[Map[Key, Value]] =
    workPool {
      val curData = mutex.synchronized {
        presentTuples = 0
        val l = queueMap.toList
        queueMap.clear
        l
      }
      val result: Map[Key, Value] = curData.flatMap {
        case (k, listV) =>
          sg.sumOption(listV).map(v => (k, v))
      }(breakOut)

      tuplesOut.incrBy(result.size)
      result
    }

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    insertOp.incr
    var newlyAddedTuples = 0

    mutex.synchronized {
      vals.foreach {
        case (k, v) =>
          val existingV = queueMap.getOrElseUpdate(k, ListBuffer[Value]())
          existingV += v
          newlyAddedTuples += 1
      }
      presentTuples += newlyAddedTuples
    }

    if (presentTuples >= bufferSize.v) {
      sizeIncr.incr
      flush
    } else
      Future.value(emptyResult)
  }
}
