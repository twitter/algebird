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
import java.util.{LinkedHashMap => JLinkedHashMap, Map => JMap}
import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._

object SummingCache {
  def apply[K, V: Semigroup](cap: Int): SummingCache[K, V] =
    new SummingCache[K, V](cap)
}

/**
 * A Stateful Summer on Map[K,V] that keeps a cache of recent keys
 */
class SummingCache[K, V](capacity: Int)(implicit sgv: Semigroup[V]) extends StatefulSummer[Map[K, V]] {

  require(capacity >= 0, "Cannot have negative capacity in SummingIterator")

  override val semigroup: MapMonoid[K, V] = new MapMonoid[K, V]
  protected def optNonEmpty(m: Map[K, V]): Option[Map[K, V]] = if (m.isEmpty) None else Some(m)

  override def put(m: Map[K, V]): Option[Map[K, V]] = {
    val replaced = m.map {
      case (k, v) =>
        val newV = cache
          .get(k)
          .map(oldV => sgv.plus(oldV, v))
          .getOrElse(v)
        (k, newV)
    }

    cache ++= replaced
    val ret = lastEvicted
    // Rest this var
    lastEvicted = Map.empty[K, V]
    optNonEmpty(ret)
  }
  override def flush: Option[Map[K, V]] = {
    // Get a copy of the cache, since it is mutable
    val res = optNonEmpty(cache.iterator.toMap)
    cache.clear
    res
  }
  override def isFlushed: Boolean = cache.isEmpty

  protected var lastEvicted: Map[K, V] = Map.empty[K, V]
  // TODO fancier caches will give better performance:
  protected lazy val cache: MMap[K, V] =
    (new JLinkedHashMap[K, V](capacity + 1, 0.75f, true) {
      override protected def removeEldestEntry(eldest: JMap.Entry[K, V]): Boolean =
        if (super.size > capacity) {
          lastEvicted += (eldest.getKey -> eldest.getValue)
          true
        } else {
          false
        }
    }).asScala
}

object SummingWithHitsCache {
  def apply[K, V: Semigroup](cap: Int): SummingWithHitsCache[K, V] =
    new SummingWithHitsCache[K, V](cap)
}

/**
 * A SummingCache that also tracks the number of key hits
 */
class SummingWithHitsCache[K, V](capacity: Int)(implicit sgv: Semigroup[V])
    extends SummingCache[K, V](capacity)(sgv) {

  def putWithHits(m: Map[K, V]): (Int, Option[Map[K, V]]) = {
    val keyHits = m.keys.count(cache.contains)
    (keyHits, put(m))
  }
}
