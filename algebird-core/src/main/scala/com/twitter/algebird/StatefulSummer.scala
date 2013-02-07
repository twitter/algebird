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
 * Law 1: Semigroup.sumOption(items) ==
 *   (Monoid.plus(items.map { stateful.put(_) }.filter { _.isDefined }, stateful.flush) &&
 *     stateful.isFlushed)
 * Law 2: isFlushed == flush.isEmpty
 * @author Oscar Boykin
 */
trait StatefulSummer[V] extends java.io.Serializable {
  def semigroup: Semigroup[V]
  // possibly emit a partially summed value
  def put(item: V): Option[V]
  // All state is reset at this point and return the rest of the sum
  def flush: Option[V]
  def isFlushed: Boolean
}
