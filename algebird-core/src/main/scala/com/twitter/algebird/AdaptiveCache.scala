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
import collection.mutable.HashMap
import ref.SoftReference

/**
 * This is a summing cache whose goal is to grow until we run out of memory,
 * at which point it clears itself and stops growing.
 * Note that we can lose the values in this cache at any point;
 * we don't put anything here we care about.
 */
class SentinelCache[K, V](implicit sgv: Semigroup[V]) {
  private val map = new SoftReference(new HashMap[K, V]())

  def size: Int = map.get.map { _.size }.getOrElse(0)

  def clear(): Unit = map.get.foreach { _.clear }

  def stopGrowing(): Unit = map.clear

  def put(in: Map[K, V]): Unit =
    if (map.get.isDefined) {
      in.foreach {
        case (k, v) =>
          val newValue =
            map.get
              .flatMap { _.get(k) }
              .map { oldV =>
                sgv.plus(oldV, v)
              }
              .getOrElse(v)

          map.get.foreach { _.put(k, newValue) }
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
class AdaptiveCache[K, V: Semigroup](maxCapacity: Int, growthMargin: Double = 3.0)
    extends StatefulSummer[Map[K, V]] {

  require(maxCapacity >= 0, "Cannot have negative capacity")
  private var currentCapacity = 1

  private var summingCache = new SummingWithHitsCache[K, V](currentCapacity)
  private val sentinelCache = new SentinelCache[K, V]

  private def update(evicted: Option[Map[K, V]]) = {
    evicted.foreach { e =>
      sentinelCache.put(e)
    }

    var ret = evicted

    if (currentCapacity < maxCapacity &&
        sentinelCache.size > (currentCapacity * growthMargin)) {
      currentCapacity = (currentCapacity * 2).min(maxCapacity)

      ret = (ret, summingCache.flush) match {
        case (Some(l), Some(r)) => Some(semigroup.plus(l, r))
        case (l, None)          => l
        case (None, r)          => r
      }

      summingCache = new SummingWithHitsCache(currentCapacity)

      if (currentCapacity == maxCapacity)
        sentinelCache.stopGrowing
      else
        sentinelCache.clear
    }
    ret
  }

  override def semigroup: MapMonoid[K, V] = summingCache.semigroup

  override def put(m: Map[K, V]): Option[Map[K, V]] =
    update(summingCache.put(m))

  override def flush: Option[Map[K, V]] = {
    val ret = summingCache.flush
    sentinelCache.clear
    ret
  }

  override def isFlushed: Boolean = summingCache.isFlushed

  private var maxReportedSentinelSize = 0
  case class CacheStats(hits: Int, cacheGrowth: Int, sentinelGrowth: Int)
  def putWithStats(m: Map[K, V]): (CacheStats, Option[Map[K, V]]) = {
    val oldCapacity = currentCapacity
    val (hits, evicted) = summingCache.putWithHits(m)
    val ret = update(evicted)
    var sentinelGrowth = 0
    if (sentinelCache.size > maxReportedSentinelSize) {
      sentinelGrowth = sentinelCache.size - maxReportedSentinelSize
      maxReportedSentinelSize = sentinelCache.size
    }
    (CacheStats(hits, currentCapacity - oldCapacity, sentinelGrowth), ret)
  }
}
