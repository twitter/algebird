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
 * @author Oscar Boykin
 */

import java.util.concurrent.ArrayBlockingQueue

import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }
import scala.collection.mutable.{ Map => MMap }
import scala.collection.JavaConverters._
import scala.annotation.tailrec

/**
 * Creates an Iterator that emits partial sums of an input Iterator[V].
 * Generally this is useful to change from processing individual Vs to
 * possibly blocks of V @see SummingQueue or a cache of recent Keys in
 * a V=Map[K,W] case: @see SummingCache
 */
object SummingIterator {
  def apply[V](summer: StatefulSummer[V], it: Iterator[V]): SummingIterator[V] =
    new SummingIterator(summer, it)

  implicit def enrich[V](it: Iterator[V]): Enriched[V] = new Enriched(it)
  /**
   * Here to add enrichments to Iterator
   */
  class Enriched[V](it: Iterator[V]) {
    def sumWith(summer: StatefulSummer[V]): SummingIterator[V] = SummingIterator(summer, it)
  }
}

class SummingIterator[V](summer: StatefulSummer[V], it: Iterator[V])
  extends java.io.Serializable with Iterator[V] {

  // This has to be lazy because it shouldn't be touched until the val it is exhausted
  protected lazy val tailIter = summer.flush.iterator
  def hasNext: Boolean = it.hasNext || tailIter.hasNext
  def next = nextInternal

  @tailrec
  private def nextInternal: V = {
    if (it.hasNext) {
      summer.put(it.next) match {
        case None => nextInternal
        case Some(v) => v
      }
    } else {
      tailIter.next
    }
  }
}
