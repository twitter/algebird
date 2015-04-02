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
 * Note that this works similar to HasAdditionOperator[Map[Int,T]] not like HasAdditionOperator[List[T]]
 * This does element-wise operations, like standard vector math, not concatenation,
 * like HasAdditionOperator[String] or HasAdditionOperator[List[T]]
 *
 * If l.size != r.size, then only sums the elements up to the index min(l.size, r.size); appends
 * the remainder to the result.
 */
class IndexedSeqHasAdditionOperator[T](implicit semi: HasAdditionOperator[T]) extends HasAdditionOperator[IndexedSeq[T]] {

  def plus(left: IndexedSeq[T], right: IndexedSeq[T]): IndexedSeq[T] = {
    // We need summands to be the same length
    val (leftSummand, rightSummand, remainder) = if (left.size > right.size) {
      (left.view(0, right.size),
        right,
        left.view(right.size, left.size))
    } else {
      (left,
        right.view(0, left.size),
        right.view(left.size, right.size))
    }

    val sum = leftSummand
      .zip(rightSummand)
      .map { tup => semi.plus(tup._1, tup._2) }

    (sum ++ remainder).toIndexedSeq
  }
}

class IndexedSeqHasAdditionOperatorAndZero[T](implicit mont: HasAdditionOperatorAndZero[T]) extends IndexedSeqHasAdditionOperator[T] with HasAdditionOperatorAndZero[IndexedSeq[T]] {
  def zero = IndexedSeq.empty[T]
  override def isNonZero(v: IndexedSeq[T]) =
    v.exists { t => mont.isNonZero(t) }
}

class IndexedSeqGroup[T](implicit grp: Group[T]) extends IndexedSeqHasAdditionOperatorAndZero[T]()(grp)
  with Group[IndexedSeq[T]] {
  override def negate(g: IndexedSeq[T]): IndexedSeq[T] = g.map { grp.negate(_) }
}

class IndexedSeqRing[T](implicit rng: Ring[T]) extends IndexedSeqGroup[T]()(rng)
  with Ring[IndexedSeq[T]] {

  // TODO
  def one = sys.error("IndexedSeqRing.one is unimplemented. It's a lot of work, and almost never used")

  def times(left: IndexedSeq[T], right: IndexedSeq[T]): IndexedSeq[T] =
    // We don't need to pad, because 0 * x = 0
    left.view
      .zip(right)
      .map { tup => rng.times(tup._1, tup._2) }
      .toIndexedSeq
}
