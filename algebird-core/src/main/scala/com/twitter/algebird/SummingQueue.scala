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

package com.twitter.algebird

/**
 * A useful utility for aggregation systems: you buffer up some number of items
 * in a thread-safe way, and when you have at most K of them, you sum them all
 * together.  A good use-case of this is doing a limited preaggregation before
 * sending on to a next phase (from mappers to reducers on Hadoop, or between
 * storm bolts).
 *
 * Without this finite buffer history, an aggregated item could build up infinite
 * history, and therefore it is unbounded in the error you could introduce by
 * losing the buffer.
 *
 * @author Ashu Singhal
 * @author Oscar Boykin
 */
import java.util.concurrent.ArrayBlockingQueue

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object SummingQueue {
  def apply[V: Semigroup](cap: Int): SummingQueue[V] = new SummingQueue(cap)
}

class SummingQueue[V] private (capacity: Int)(override implicit val semigroup: Semigroup[V])
    extends StatefulSummer[V] {

  private val queueOption: Option[ArrayBlockingQueue[V]] =
    if (capacity > 0) Some(new ArrayBlockingQueue[V](capacity, true)) else None

  /**
   * puts an item to the queue, optionally sums up the queue and returns value
   * This never blocks interally. It uses offer. If the queue is full, we drain,
   * sum the queue.
   */
  override final def put(item: V): Option[V] =
    if (queueOption.isDefined) {
      queueOption.flatMap { queue =>
        if (!queue.offer(item)) {
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
  override def flush: Option[V] =
    queueOption.flatMap { queue =>
      val toSum = ListBuffer[V]()
      queue.drainTo(toSum.asJava)
      Semigroup.sumOption(toSum)
    }
  override def isFlushed: Boolean = queueOption.map { _.size == 0 }.getOrElse(true)
}
