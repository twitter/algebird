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

import java.util.{LinkedHashMap => JLinkedHashMap}
import java.util.{Map => JMap}

import scala.Option.option2Iterable
import scala.annotation.implicitNotFound
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.{Map => MMap}

object SummingCache {
  /**
   * returns the default SummingCache implementation: PlusOptimizedSummingCache
   */
  def apply[K,V:Semigroup](cap: Int): SummingCache[K,V] = PlusOptimizedSummingCache(cap)
}

object SumOptionOptimizedSummingCache {
  def apply[K,V:Semigroup](cap: Int): SummingCache[K,V] = new SumOptionOptimizedSummingCache[K,V](cap)
}

object PlusOptimizedSummingCache {
  def apply[K,V:Semigroup](cap: Int): SummingCache[K,V] = new PlusOptimizedSummingCache[K,V](cap)
}

trait SummingCache[K,V] extends StatefulSummer[Map[K,V]]

/** A Stateful Summer on Map[K,V] that keeps a cache of recent keys
 *  @author Oscar Boykin
 */
class PlusOptimizedSummingCache[K,V] private (capacity: Int)(implicit sgv: Semigroup[V])
  extends SummingCache[K,V] {

  require(capacity >= 0, "Cannot have negative capacity in SummingIterator")

  override val semigroup = new MapMonoid[K,V]
  protected def optNonEmpty(m: Map[K,V]) = if (m.isEmpty) None else Some(m)

  override def put(m: Map[K,V]): Option[Map[K,V]] = {
    val replaced = m.map { case (k, v) =>
      val newV = cache.get(k)
        .map { oldV => sgv.plus(oldV, v) }
        .getOrElse { v }
      (k, newV)
    }

    cache ++= replaced
    val ret = lastEvicted
    // Rest this var
    lastEvicted = Map.empty[K,V]
    optNonEmpty(ret)
  }
  override def flush: Option[Map[K,V]] = {
    //Get a copy of the cache, since it is mutable
    val res = optNonEmpty(Map(cache.toSeq: _*))
    cache.clear
    res
  }
  def isFlushed = cache.isEmpty

  protected var lastEvicted: Map[K,V] = Map.empty[K,V]
  // TODO fancier caches will give better performance:
  protected lazy val cache: MMap[K,V] = (new JLinkedHashMap[K,V](capacity + 1, 0.75f, true) {
      override protected def removeEldestEntry(eldest : JMap.Entry[K, V]) =
        if(super.size > capacity) {
          lastEvicted += (eldest.getKey -> eldest.getValue)
          true
        }
        else {
          false
        }
    }).asScala
}


/** A Stateful Summer on Map[K,V] that keeps a cache of recent keys
 *  @author Julien Le Dem
 */
class SumOptionOptimizedSummingCache[K,V] private (capacity: Int)(implicit sgv: Semigroup[V])
  extends SummingCache[K,V] {

  require(capacity >= 0, "Cannot have negative capacity in SummingIterator")

  override val semigroup = new MapMonoid[K,V]

  private var presentTuples = 0
  private val cache = MMap[K, ListBuffer[V]]()

  override def put(m: Map[K,V]): Option[Map[K,V]] = {
    m.foreach { case (k, v) =>
        cache.getOrElseUpdate(k, ListBuffer[V]()) += v
      }
    presentTuples += m.size

    // if the number of distinct keys fits in the buffer, we just aggregate values
    if (presentTuples >= capacity && cache.size < capacity) aggregateCache

    // if they don't we need to flush
    if (presentTuples >= capacity)
      flush
    else
      None
  }

  private def sum(items: Seq[V]): Option[V] = {
    if (items.size == 1) {
      Some(items.head)
    } else {
      sgv.sumOption(items)
    }
  }

  /**
   * aggregates each value list in the cache in place
   */
  private def aggregateCache() = {
    var newCount = 0
    cache.foreach { case (k, listV) =>
      if (listV.size > 2) { // we don't want to aggregate if the list is not big enough
        val sum = sgv.sumOption(listV)
        listV.clear
        sum.foreach((v) => listV += v)
      }
      newCount += listV.size
    }
    presentTuples = newCount
  }

  /**
   * aggregate and write the cache to the output
   */
  override def flush: Option[Map[K,V]] = {
    val flushed = cache.toMap.flatMap{ case (k, listV) =>
        sum(listV).map(v => (k, v))
      }
     presentTuples = 0
     cache.clear
     if (flushed.isEmpty) None else Some(flushed)
  }

  def isFlushed = presentTuples == 0

}
