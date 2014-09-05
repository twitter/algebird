/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.algebird.mutable

/**
 * This is a mutable LRU that is GC friendly: references are nulled out after they
 * are not needed, we recycle evicted nodes so we don't need to allocate much,
 * we minimize use of Option in favor of old-school null except at the API boundaries.
 *
 * The reason we do this is because a cache has to be very fast or otherwise it can
 * use more resources than it saves. In the case of scalding, GC pressure created by
 * the standard Java LRU makes performance WORSE using the cache than not because of
 * the huge amount of garbarge created.
 */
class LRU[K, V](capacity: Int) {
  require(capacity >= 0, s"Capacity must be >= 0. Found: ${capacity}")
  private class LinkedNode[T <: AnyRef] {
    var item: T = _
    var prev: LinkedNode[T] = _
    var next: LinkedNode[T] = _

    def unlink(): Unit = {
      if (prev != null) prev.next = next
      if (next != null) next.prev = prev
      prev = null
      next = null
    }
  }

  /*
   * Use an open hashmap which should be more memory friendly
   * should be faster than Java:
   * https://gist.github.com/pchiusano/1423303
   */
  private val map = new java.util.HashMap[K, LinkedNode[(K, V)]](capacity)

  private var oldest: LinkedNode[(K, V)] = _
  private var newest: LinkedNode[(K, V)] = _

  // Recycle references to reduce GC pressure
  private var freeNodes: LinkedNode[(K, V)] = _
  private def free(linked: LinkedNode[(K, V)]) {
    linked.item = null
    linked.prev = null
    linked.next = freeNodes
    freeNodes = linked
  }
  private def alloc: LinkedNode[(K, V)] = freeNodes match {
    case null => new LinkedNode[(K, V)]
    case freeNode =>
      freeNodes = freeNode.next
      freeNode.next = null
      freeNode
  }

  private def evictOldest: Option[(K, V)] = oldest match {
    case null => None
    case linked =>
      oldest = linked.next
      // newest might be the oldest
      if (newest == linked) { newest = null }
      val res = linked.item
      map.remove(res._1)
      free(linked)
      Some(res)
  }

  // Check the value, but don't change the LRU ordering
  def peek(k: K): Option[V] =
    // Scala throws an exception on getting from capacity 0
    if (capacity == 0) None
    else map.get(k) match {
      case null => None
      case node => Some(node.item._2)
    }

  // Update and maybe evict an old node
  def update(k: K)(fn: Option[V] => V): Option[(K, V)] =
    if (capacity == 0) Some((k, fn(None)))
    else map.get(k) match {
      case null => // New node
        val evicted = if (map.size >= capacity) evictOldest else None
        val newln = alloc
        if (newest != null) { newest.next = newln }
        newln.item = (k, fn(None))
        newln.prev = newest
        newest = newln
        // We are the newest and the oldest
        if (oldest == null) { oldest = newln }
        // Point the key at this node
        map.put(k, newln)
        evicted
      case existing =>
        def updateNewest: Unit = {
          existing.unlink
          newest.next = existing
          existing.prev = newest
          newest = existing
        }
        // If the item exists, we don't change the hashmap, just the linked list
        if (existing == newest) {
          // Don't move the node, already the newest
        } else if (existing == oldest) {
          // More than one node because we are the oldest but not newest
          oldest = existing.next
          updateNewest
        } else {
          updateNewest
        }
        existing.item = (k, fn(Some(existing.item._2)))
        None
    }
  // Returns the items from oldest to newest
  def clear: IndexedSeq[(K, V)] = {
    val bldr = IndexedSeq.newBuilder[(K, V)]
    var tempNode = oldest
    while (tempNode != null) {
      bldr += tempNode.item
      val oldNext = tempNode.next
      // Null out, as these might be big
      tempNode.item = null
      tempNode.prev = null
      tempNode.next = null
      tempNode = oldNext
    }
    map.clear
    freeNodes = null
    oldest = null
    newest = null
    bldr.result()
  }

  def isEmpty = map.isEmpty
}
