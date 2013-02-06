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

import scala.annotation.tailrec
import scala.collection.SortedSet

/** A sorted set monoid
 */
class SortedSetMonoid[T](max : Int = Int.MaxValue)(implicit ord : Ordering[T]) extends Monoid[SortedSet[T]] {
  override lazy val zero = SortedSet[T]()
  override def plus(left : SortedSet[T], right : SortedSet[T]): SortedSet[T] = {
    // convert toLong to deal with overflow
    if(left.size.toLong + right.size.toLong <= max) {
      left ++ right
    }
    else {
      // We can't fit all of them:
      val (lower, upper) = if (ord.lt(left.min, right.min)) {
          (left, right)
        }
        else if (ord.lt(right.min, left.min)) {
          (right, left)
        }
        else {
          one should be the one closest to the size of max
        }
      if( one.max <= other.min) {
        //Just take as many as you need and put them in:
        one ++ (other.iterator.take(max - one.size))
      }
      else {
       //Find the three partitions (one|one,other|other), and then take in that order
      }
    }
  }
}
