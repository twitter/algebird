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
package com.twitter.algebird.mutable

import com.twitter.algebird.Monoid

import java.util.PriorityQueue

/**
 * for sort-with take and better performance over large values The priority queues should be MAX queues, i.e.
 * the ones we want least should be in the .peek position This is MUCH Faster for Top-K algorithms Note this
 * is MUTABLE. When you put something in plus, it is changed!
 */
class PriorityQueueMonoid[K](max: Int)(implicit ord: Ordering[K]) extends Monoid[PriorityQueue[K]] {

  require(max > 0, "PriorityQueueMonoid requires keeping at least 1 item")
  // Java throws if you try to make a queue size 0
  protected val MINQUEUESIZE = 1
  def build(k: K): PriorityQueue[K] = {
    val q = new PriorityQueue[K](1, ord.reverse);
    q.add(k)
    q
  }
  def build(items: Iterable[K]): PriorityQueue[K] = {
    val q = new PriorityQueue(items.size.max(MINQUEUESIZE), ord.reverse);
    items.foreach { item =>
      if (q.size < max || ord.lteq(item, q.peek)) {
        q.add(item)
      }
    }
    limit(q)
    q
  }
  protected def limit(q: PriorityQueue[K]): Unit =
    while (q.size > max) { q.poll() }

  override def zero: PriorityQueue[K] = new PriorityQueue[K](MINQUEUESIZE, ord.reverse)
  override def isNonZero(q: PriorityQueue[K]): Boolean = q.size > 0

  override def plus(left: PriorityQueue[K], right: PriorityQueue[K]): PriorityQueue[K] = {
    val (bigger, smaller) =
      if (left.size >= right.size) (left, right) else (right, left)
    var biggest = bigger.peek

    var next = smaller.poll
    while (next != null) {
      if (bigger.size < max) {
        // we may have increased the biggest value:
        biggest = ord.max(biggest, next)
        bigger.add(next)
      } else if (ord.lteq(next, biggest)) {
        // this cannot increase the biggest
        bigger.add(next)
      }
      next = smaller.poll
    }
    limit(bigger)
    bigger
  }
}
