/*
Copyright 2015 Twitter, Inc.

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
 * @author Avi Bryant
 */

import collection.mutable.{ ListBuffer, HashMap }
import ref.{ SoftReference, ReferenceQueue }

/**
 * This is a summing cache whose goal is to grow until we run out of memory,
 * at which point it clears itself and stops growing.
 * Note that we can lose the values in this cache at any point;
 * we don't put anything here we care about.
 */
class SentinelCache[K, V](implicit sgv: Semigroup[V]) {
  private val queue = new ref.ReferenceQueue
  private val map = new HashMap[K, ref.SoftReference[ListBuffer[V]]]()
  private var grow = true

  def size = map.size

  def clear { map.clear }

  def stopGrowing {
    grow = false
    clear
  }

  def put(in: Map[K, V]) {
    if (grow) {
      if (queue.poll.isDefined) {
        stopGrowing
      } else {
        in.foreach {
          case (k, v) =>
            map.get(k).flatMap { _.get } match {
              case Some(buf) =>
                buf.update(0, sgv.plus(buf(0), v))
              case _ =>
                map.put(k, new ref.SoftReference(ListBuffer(v), queue))
            }
        }
      }
    }
  }
}

/**
 * This is a wrapper around SummingCache that attempts to grow the capacity
 * by up to some maximum, as long as there's enough RAM.
 * It determines that there's enough RAM to grow by maintaining a SentinelCache
 * which keeps caching and summing the evicted values.
 * Once the SentinelCache has grown to the same size as the current cache,
 * plus some margin, without running out of RAM, then this indicates that we
 * have enough headroom to double the capacity.
 */
class AdaptiveCache[K, V: Semigroup](maxCapacity: Int, growthMargin: Double = 1.5)
  extends StatefulSummer[Map[K, V]] {

  require(maxCapacity >= 0, "Cannot have negative capacity")
  private var currentCapacity = 1

  private var summingCache = new SummingCache[K, V](currentCapacity)
  private val sentinelCache = new SentinelCache[K, V]

  override def semigroup = summingCache.semigroup

  override def put(m: Map[K, V]): Option[Map[K, V]] = {
    var ret = summingCache.put(m)
    ret.foreach{ e => sentinelCache.put(e) }

    if (currentCapacity < maxCapacity &&
      sentinelCache.size > (currentCapacity * growthMargin)) {
      currentCapacity = (currentCapacity * 2).min(maxCapacity)

      ret = (ret, summingCache.flush) match {
        case (Some(l), Some(r)) => Some(semigroup.plus(l, r))
        case (l, None) => l
        case (None, r) => r
      }

      summingCache = new SummingCache(currentCapacity)

      if (currentCapacity == maxCapacity)
        sentinelCache.stopGrowing
    }

    ret
  }

  override def flush: Option[Map[K, V]] = {
    val res = summingCache.flush
    sentinelCache.clear
    res
  }

  def isFlushed = summingCache.isFlushed
}
