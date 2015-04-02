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
 * A Stateful summer is something that is potentially more efficient
 * (a buffer, a cache, etc...) that has the same result as a sum:
 * Law 1: HasAdditionOperator.sumOption(items) ==
 *   (HasAdditionOperatorAndZero.plus(items.map { stateful.put(_) }.filter { _.isDefined }, stateful.flush) &&
 *     stateful.isFlushed)
 * Law 2: isFlushed == flush.isEmpty
 * @author Oscar Boykin
 */
trait StatefulSummer[V] extends Buffered[V, V] {
  def semigroup: HasAdditionOperator[V]
}

/**
 * Sum the entire iterator one item at a time. Only emits on flush
 * you should probably prefer BufferedSumAll
 */
class SumAll[V](implicit override val semigroup: HasAdditionOperator[V]) extends StatefulSummer[V] {
  var summed: Option[V] = None
  def put(item: V) = {
    summed = HasAdditionOperator.plus(summed, Some(item))
    None
  }
  def flush = {
    val res = summed
    summed = None
    res
  }
  def isFlushed = summed.isEmpty
}

class BufferedSumAll[V](size: Int)(implicit override val semigroup: HasAdditionOperator[V])
  extends ArrayBufferedOperation[V, V](size)
  with StatefulSummer[V]
  with BufferedReduce[V] {

  def operate(nonEmpty: Seq[V]): V = semigroup.sumOption(nonEmpty).get
}
