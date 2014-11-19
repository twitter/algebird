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

import com.twitter.algebird.MonoidAggregator
import scala.collection.JavaConverters._

import java.util.PriorityQueue

/**
 * This gives you the `max` smallest items. If you want the biggest reverse the Ordering.
 * Note that PriorityQueue is mutable so it is a good idea to copy this into
 * an immutable view before using it, as is done in PriorityQueueToListAggregator
 */
abstract class PriorityQueueAggregator[A, +C](max: Int)(implicit ord: Ordering[A])
  extends MonoidAggregator[A, C] {
  type B = PriorityQueue[A]
  /*
   you need to override:
  def present(q: PriorityQueue[A]): B
   */

  val monoid = new PriorityQueueMonoid[A](max)
  final def prepare(a: A) = monoid.build(a)
}

/**
 * Should probably be your default Top-K implementation
 */
class PriorityQueueToListAggregator[A](max: Int)(implicit ord: Ordering[A])
  extends PriorityQueueAggregator[A, List[A]](max) {
  def present(q: PriorityQueue[A]) = q.iterator.asScala.toList.sorted
}

