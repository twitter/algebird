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

/** A sorted-take List monoid (not the default, you can set:
 * implicit val sortmon = new SortedTakeListMonoid[T](10)
 * to use this instead of the standard list
 * This returns the k least values:
 * equivalent to: (left ++ right).sorted.take(k)
 * but doesn't do a total sort
 * NOTE!!!! This assumes the inputs are already sorted! resorting each time kills speed
 */
class SortedTakeListMonoid[T](k : Int)(implicit ord : Ordering[T]) extends Monoid[List[T]] {
  override def zero = List[T]()
  override def plus(left : List[T], right : List[T]) : List[T] = {
    //This is the internal loop that does one comparison:
    @tailrec
    def mergeSortR(acc : List[T], list1 : List[T], list2 : List[T], cnt : Int) : List[T] = {
      (list1, list2, cnt) match {
        case (_, _, 0) => acc
        case (x1 :: t1, x2 :: t2, _) => {
          if( ord.lt(x1,x2) ) {
            mergeSortR(x1 :: acc, t1, list2, cnt-1)
          }
          else {
            mergeSortR(x2 :: acc, list1, t2, cnt-1)
          }
        }
        case (x1 :: t1, Nil, _) => mergeSortR(x1 :: acc, t1, Nil, cnt-1)
        case (Nil, x2 :: t2, _) => mergeSortR(x2 :: acc, Nil, t2, cnt-1)
        case (Nil, Nil, _) => acc
      }
    }
    mergeSortR(Nil, left, right, k).reverse
  }
}
