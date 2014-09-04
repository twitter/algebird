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

import com.twitter.algebird.mutable.LRU

/**
 * @author Oscar Boykin
 */

object SummingCache {
  def apply[K, V: Semigroup](cap: Int): SummingCache[K, V] = new SummingCache[K, V](cap)
}
/**
 * A Stateful Summer on Map[K,V] that keeps a cache of recent keys
 */
class SummingCache[K, V] private (capacity: Int)(implicit sgv: Semigroup[V])
  extends StatefulSummer[Map[K, V]] {

  require(capacity >= 0, "Cannot have negative capacity in SummingCache")

  override val semigroup = new MapMonoid[K, V]
  protected def optNonEmpty(m: Map[K, V]) = if (m.isEmpty) None else Some(m)

  override def put(m: Map[K, V]): Option[Map[K, V]] = {
    val bldr = Seq.newBuilder[(K, V)]
    m.foreach {
      case (k, v) =>
        val newV = cache.peek(k)
          .map { oldV => sgv.plus(oldV, v) }
          .getOrElse { v }

        cache.update((k, newV)).foreach { bldr += _ }
    }

    optNonEmpty(MapAlgebra.sumByKey(bldr.result()))
  }
  override def flush: Option[Map[K, V]] =
    optNonEmpty(cache.clear.toMap)

  def isFlushed = cache.isEmpty

  protected val cache: LRU[K, V] = new LRU(capacity)
}

