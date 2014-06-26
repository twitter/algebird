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
import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 * @author Ian O Connell
 */

class AsyncMapSum[Key, Value](bufferSize: BufferSize,
  override val flushFrequency: FlushFrequency,
  override val softMemoryFlush: MemoryFlushPercent,
  workPool: FuturePool)(implicit semigroup: Semigroup[Value])
  extends AsyncSummer[(Key, Value), Map[Key, Value]]
  with WithFlushConditions[(Key, Value), Map[Key, Value]] {

  require(bufferSize.v > 0, "Use the Null summer for an empty async summer")

  protected override val emptyResult = Map.empty[Key, Value]

  private[this] final val queue = new ArrayBlockingQueue[Map[Key, Value]](bufferSize.v, true)
  override def isFlushed: Boolean = queue.size == 0

  override def flush: Future[Map[Key, Value]] = {
    val toSum = ArrayBuffer[Map[Key, Value]]()
    queue.drainTo(toSum.asJava)
    workPool {
      Semigroup.sumOption(toSum).getOrElse(Map.empty)
    }
  }

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    val curData = Semigroup.sumOption(vals.map(Map(_))).getOrElse(Map.empty)
    if (!queue.offer(curData)) {
      flush.map { flushRes =>
        Semigroup.plus(flushRes, curData)
      }
    } else {
      Future.value(Map.empty)
    }
  }
}