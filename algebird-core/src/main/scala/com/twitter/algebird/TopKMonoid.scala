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

case class TopK[N](size: Int, items: List[N], max: Option[N])

object TopKMonoid extends java.io.Serializable {
  // Does a merge sort and returns the reversed list
  @tailrec
  private[algebird] def mergeSortR[T](acc: List[T], list1: List[T], list2: List[T], cnt: Int)(
      implicit ord: Ordering[T]): List[T] =
    (list1, list2, cnt) match {
      case (_, _, 0) => acc
      case (x1 :: t1, x2 :: t2, _) => {
        if (ord.lt(x1, x2)) {
          mergeSortR(x1 :: acc, t1, list2, cnt - 1)
        } else {
          mergeSortR(x2 :: acc, list1, t2, cnt - 1)
        }
      }
      case (x1 :: t1, Nil, _) => mergeSortR(x1 :: acc, t1, Nil, cnt - 1)
      case (Nil, x2 :: t2, _) => mergeSortR(x2 :: acc, Nil, t2, cnt - 1)
      case (Nil, Nil, _)      => acc
    }
}

/**
 * A top-k monoid that is much faster than SortedListTake
 * equivalent to: (left ++ right).sorted.take(k)
 * but doesn't do a total sort
 * If you can handle the mutability, mutable.PriorityQueueMonoid is even faster.
 *
 * NOTE!!!! This assumes the inputs are already sorted! resorting each time kills speed
 */
class TopKMonoid[T](k: Int)(implicit ord: Ordering[T]) extends Monoid[TopK[T]] {

  require(k > 0, "TopK requires at least K>0")

  override lazy val zero = TopK[T](0, List[T](), None)

  def build(t: T): TopK[T] = TopK(1, List(t), Some(t))
  def build(ts: Iterable[T]): TopK[T] = ts.foldLeft(zero) { (acc, t) =>
    plus(acc, build(t))
  }

  override def plus(left: TopK[T], right: TopK[T]): TopK[T] = {
    val (bigger, smaller) =
      if (left.size >= right.size) (left, right) else (right, left)
    if (smaller.size == 0) {
      bigger
    } else if (bigger.size == k) {
      // See if we can just return the bigger:
      val biggerWins =
        for {
          biggest <- bigger.max
          smallest <- smaller.items.headOption
        } yield (ord.lteq(biggest, smallest))
      if (biggerWins.getOrElse(true)) { // smaller is small, or empty
        bigger
      } else {
        merge(bigger, smaller)
      }
    } else {
      merge(bigger, smaller)
    }
  }
  protected def merge(bigger: TopK[T], smaller: TopK[T]): TopK[T] = {
    import TopKMonoid.mergeSortR
    // This is the internal loop that does one comparison:
    val newItems = mergeSortR(Nil, bigger.items, smaller.items, k)
    val max = newItems.headOption
    // Now reverse and get the size:
    val (size, reversed) = newItems.foldLeft((0, List[T]())) { (cntItems, v) =>
      val (olds, oldl) = cntItems
      (olds + 1, v :: oldl)
    }
    TopK(size, reversed, max)
  }
}

class TopKToListAggregator[A](max: Int)(implicit ord: Ordering[A])
    extends MonoidAggregator[A, TopK[A], List[A]] {
  val monoid: Monoid[TopK[A]] = new TopKMonoid[A](max)(ord)
  override def present(a: TopK[A]): List[A] = a.items
  override def prepare(a: A): TopK[A] = TopK(1, List(a), Some(a))
}
