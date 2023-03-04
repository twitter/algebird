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

import java.util.concurrent.ArrayBlockingQueue

import com.twitter.algebird._
import com.twitter.util.Future

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.compat._

/**
 * @author
 *   Ian O Connell
 */
case class SyncSummingQueue[Key, Value](
    bufferSize: BufferSize,
    override val flushFrequency: FlushFrequency,
    override val softMemoryFlush: MemoryFlushPercent,
    override val memoryIncr: Incrementor,
    override val timeoutIncr: Incrementor,
    sizeIncr: Incrementor,
    insertOps: Incrementor,
    tuplesIn: Incrementor,
    tuplesOut: Incrementor
)(implicit semigroup: Semigroup[Value])
    extends AsyncSummer[(Key, Value), Map[Key, Value]]
    with WithFlushConditions[(Key, Value), Map[Key, Value]] {

  require(bufferSize.v > 0, "Use the Null summer for an empty async summer")
  protected override val emptyResult = Map.empty[Key, Value]

  private[this] final val squeue: CustomSummingQueue[Map[Key, Value]] =
    new CustomSummingQueue(bufferSize.v, sizeIncr, insertOps)
  override def isFlushed: Boolean = squeue.isFlushed

  def flush: Future[Map[Key, Value]] = {
    val tups = squeue.flush.getOrElse(Map.empty)
    tuplesOut.incrBy(tups.size)
    Future.value(tups)
  }

  def addAll(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    val outputs = squeue
      .put(Monoid.sum(vals.iterator.map { i =>
        tuplesIn.incr()
        Map(i)
      }))
      .getOrElse(Map.empty)

    tuplesOut.incrBy(outputs.size)
    Future.value(outputs)
  }
}

class CustomSummingQueue[V](capacity: Int, sizeIncr: Incrementor, putCalls: Incrementor)(
    override implicit val semigroup: Semigroup[V]
) extends StatefulSummer[V] {

  private val queueOption: Option[ArrayBlockingQueue[V]] =
    if (capacity > 0) Some(new ArrayBlockingQueue[V](capacity, true)) else None

  /**
   * puts an item to the queue, optionally sums up the queue and returns value This never blocks interally. It
   * uses offer. If the queue is full, we drain, sum the queue.
   */
  final def put(item: V): Option[V] =
    if (queueOption.isDefined) {
      putCalls.incr()
      queueOption.flatMap { queue =>
        if (!queue.offer(item)) {
          sizeIncr.incr()
          // Queue is full, do the work:
          Monoid.plus(flush, Some(item))
        } else {
          // We are in the queue
          None
        }
      }
    } else {
      Some(item)
    }

  def apply(v: V): Option[V] = put(v)

  /**
   * drain the queue and return the sum. If empty, return None
   */
  def flush: Option[V] =
    queueOption.flatMap { queue =>
      val toSum = ListBuffer[V]()
      queue.drainTo(toSum.asJava)
      Semigroup.sumOption(toSum)
    }
  def isFlushed: Boolean = queueOption.map(_.size == 0).getOrElse(true)
}
