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

/** Note that this works similar to Monoid[Map[Int,T]] not like Monoid[List[T]]
 * This does element-wise operations, like standard vector math, not concatenation,
 * like Monoid[String] or Monoid[List[T]]
 */
class IndexedSeqMonoid[T](implicit mont: Monoid[T]) extends Monoid[IndexedSeq[T]] {
  def zero = IndexedSeq.empty[T]
  override def isNonZero(v: IndexedSeq[T]) = v.exists { t => mont.isNonZero(t) }
  // We append mont.zero on the shorter of the two inputs:
  def plus(left: IndexedSeq[T], right: IndexedSeq[T]): IndexedSeq[T] = {
    // We need them to be the same length:
    val maxSize = scala.math.max(left.size, right.size)
    def pad(v: IndexedSeq[T]) = {
      val diff = maxSize - v.size
      if(diff > 0) {
        v ++ (Iterator.fill(diff)(mont.zero))
      }
      else {
        v
      }
    }
    pad(left)
      .view
      .zip(pad(right))
      .map { tup => mont.plus(tup._1, tup._2) }
      .toIndexedSeq
  }
}

class IndexedSeqGroup[T](implicit grp: Group[T]) extends IndexedSeqMonoid[T]()(grp)
  with Group[IndexedSeq[T]] {
  override def negate(g: IndexedSeq[T]): IndexedSeq[T] = g.map { grp.negate(_) }
}

class IndexedSeqRing[T](implicit rng: Ring[T]) extends IndexedSeqGroup[T]()(rng)
  with Ring[IndexedSeq[T]] {

  // TODO
  def one = error("IndexedSeqRing.one is unimplemented. It's a lot of work, and almost never used")

  def times(left: IndexedSeq[T], right: IndexedSeq[T]): IndexedSeq[T] =
    // We don't need to pad, because 0 * x = 0
    left.view
      .zip(right)
      .map { tup => rng.times(tup._1, tup._2) }
      .toIndexedSeq
}

