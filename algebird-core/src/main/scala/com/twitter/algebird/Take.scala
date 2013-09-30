/*
Copyright 2013 Twitter, Inc.

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

object TakeState {
  def apply[T](item: T): TakeState[T] = TakeState(item, false)
}

case class TakeState[+T](item: T, emitted: Boolean)

/** Assumes you put the old item on the left
 * To get the normal List.take behavior, use this with the Last semigroup:
 * e.g. val sg = new TakeSemigroup[Last[T]](100)
 */
class TakeSemigroup[T](count: Long)(implicit sgT: Semigroup[T]) extends TakeWhileSemigroup[(T, Long)] {
  assert(count > 0, "TakeSemigroup only makes sense if you take at least one")
  def isDone(item: (T, Long)) = {
    val (l, cnt) = item
    (cnt > count)
  }
}

object TakeWhileSemigroup {
  def apply[T](fn: T => Boolean)(implicit sg: Semigroup[T]): TakeWhileSemigroup[T] =
    new TakeWhileSemigroup()(sg) {
      def isDone(item: T) = !fn(item)
    }
}
/** Assumes you put the old item on the left
 *
 * To get a threshold, use a sum and takeWhile:
 * val sg = new TakeWhileSemigroup[Long](_ < 10000L)
 * sg.sumOption(items: List[Long]).collect{case TakeState(t, false) => t}
 *
 * NOTE: for this to be a valid semigroup (and get the desired properties on parallelism)
 * you need isDone to have the property that
 * if isDone(a) || isDone(b) then isDone(a+b)
 * so, isDone is monotonic in adding the valid elements of T is the Semigroup[T].
 */
abstract class TakeWhileSemigroup[T](implicit sgT: Semigroup[T]) extends Semigroup[TakeState[T]] {
  /**
   * given and item, are we done taking, and if so emit a normalized value
   */
  def isDone(item: T): Boolean

  def plus(left: TakeState[T], right: TakeState[T]): TakeState[T] = {
    val TakeState(t2, b2) = left
    val TakeState(t1, b1) = right
    val nextT = sgT.plus(t2, t1)
    TakeState(nextT, b2 || b1 || isDone(nextT))
  }
}

