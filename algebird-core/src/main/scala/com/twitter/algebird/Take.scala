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
  def apply[T](item: T): TakeState[T] = TakeState(item, 1L, false)
}

case class TakeState[+T](item: T, count: Long, emitted: Boolean)

/** Assumes you put the old item on the left
 */
class TakeSemigroup[T](count: Long) extends Semigroup[TakeState[T]] {
  assert(count > 0, "TakeSemigroup only makes sense if you take at least one")

  def plus(left: TakeState[T], right: TakeState[T]): TakeState[T] = {
    val TakeState(t2, c2, b2) = left
    val TakeState(t1, c1, b1) = right
    if(b2 || (c2 > count)) { // take the old one:
      TakeState(t2, count, true)
    }
    else {
      //Have not stopped yet:
      val newC = c1 + c2
      if(newC > count)
        TakeState(t1, count, true)
      else
        TakeState(t1, newC, false)
    }
  }
}
