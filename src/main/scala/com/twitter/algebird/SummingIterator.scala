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
import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._
import scala.annotation.tailrec

object SummingIterator {
  def apply[K,V:Semigroup](maxKeysInMemory: Int, it: Iterator[(K,V)]): SummingIterator[K,V] =
    new SummingIterator(maxKeysInMemory, it)
}

/**
 * doesn't preserve any ordering on the keys, but values are summed in the order
 * they are seen
 */
class SummingIterator[K,V:Semigroup] private (capacity: Int, it: Iterator[(K,V)])
  extends java.io.Serializable with Iterator[(K,V)] {

  require(capacity >= 0, "Cannot have negative capacity in SummingIterator")

  // It's always cheap to check cache.size, do it first
  def hasNext: Boolean = (cache.size > 0) || it.hasNext
  def next = nextInternal

  @tailrec
  private def nextInternal: (K,V) = {
    if(it.hasNext) {
      put(it.next) match {
        case None => nextInternal
        case Some(tup) => tup
      }
    }
    else {
      // Just take one from the cache
      val head = cache.head
      cache -= head._1
      head
    }
  }

  protected def put(tup: (K,V)): Option[(K,V)] = {
    val newV = cache.get(tup._1)
      .map { v => Semigroup.plus(v, tup._2) }
      .getOrElse { tup._2 }

    cache += tup._1 -> newV
    val ret = lastEvicted
    // Rest this var
    lastEvicted = None
    ret
  }
  protected var lastEvicted: Option[(K,V)] = None
  // TODO fancier caches will give better performance:
  protected lazy val cache: MMap[K,V] = (new JLinkedHashMap[K,V](capacity + 1, 0.75f, true) {
      override protected def removeEldestEntry(eldest : JMap.Entry[K, V]) =
        if(super.size > capacity) {
          lastEvicted = Some(eldest.getKey, eldest.getValue)
          true
        }
        else {
          lastEvicted = None
          false
        }
    }).asScala
}
