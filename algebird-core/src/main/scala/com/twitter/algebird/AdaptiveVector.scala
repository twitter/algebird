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

/** Some functions to create or convert AdaptiveVectors
 */
object AdaptiveVector {
  /** When density >= this value * size, we switch to dense vectors
   */
  val THRESHOLD = 0.25
  def fill[V](size: Int)(sparse: V): AdaptiveVector[V] = SparseVector(Map.empty[Int,V], sparse, size)

  def fromVector[V](v: Vector[V], sparseVal: V): AdaptiveVector[V] = {
    if(v.size == 0) {
      fill[V](0)(sparseVal)
    }
    else {
      val denseCount = v.count { _ != sparseVal }
      val sz = v.size
      if(denseCount < sz * THRESHOLD)
        SparseVector(toMap(v, sparseVal), sparseVal, sz)
      else
        DenseVector(v, sparseVal, denseCount)
    }
  }
  def fromMap[V](m: Map[Int, V], sparseVal: V, sizeOfDense: Int): AdaptiveVector[V] = {
    if(m.size == 0) {
      fill[V](sizeOfDense)(sparseVal)
    }
    else {
      val maxIdx = m.keys.max
      require(maxIdx < sizeOfDense, "Max key (" + maxIdx +") exceeds valid for size (" + sizeOfDense +")")
      val denseCount = m.count { _._2 != sparseVal }
      if(denseCount < sizeOfDense * THRESHOLD)
        SparseVector(m, sparseVal, sizeOfDense)
      else
        DenseVector(toVector(m, sparseVal, sizeOfDense), sparseVal, denseCount)
    }
  }

  def toMap[V](v: AdaptiveVector[V]): Map[Int, V] =
    v match {
      case DenseVector(is, sv, _) => toMap(is, sv)
      case SparseVector(m, _, _) => m
    }

  def toMap[V](iseq: IndexedSeq[V], sparse: V): Map[Int, V] =
    iseq.view.zipWithIndex.filter { _._1 != sparse }.map { _.swap }.toMap

  def toVector[V](m: Map[Int,V], sparse: V, size: Int): Vector[V] = {
    // Mutable local variable to optimize performance
    import scala.collection.mutable.Buffer
    val buf = Buffer.fill[V](size)(sparse)
    m.foreach { case (idx, v) => buf(idx) = v }
    Vector( buf :_* )
  }

  def toVector[V](v: AdaptiveVector[V]): Vector[V] =
    v match {
      case DenseVector(is, sv, _) => is
      case SparseVector(m, v, sz) => toVector(m, v, sz)
    }

  private def withSparse[V](v: AdaptiveVector[V], sv: V): AdaptiveVector[V] =
    if(v.sparseValue == sv) v
    else fromVector(toVector(v), sv)

  private class AVSemigroup[V:Semigroup] extends Semigroup[AdaptiveVector[V]] {
    private def valueIsNonZero(v: V): Boolean = implicitly[Semigroup[V]] match {
      case m: Monoid[_] => m.isNonZero(v)
      case _ => true
    }

    def plus(left: AdaptiveVector[V], right: AdaptiveVector[V]) = {
      if(left.sparseValue != right.sparseValue) {
        if(left.denseCount > right.denseCount) plus(withSparse(left, right.sparseValue), right)
        else plus(left, withSparse(right, left.sparseValue))
      }
      else {
        // they have the same sparse value
        val maxSize = Ordering[Int].max(left.size, right.size)
        (left, right) match {
          case (DenseVector(lv, ls, ld), DenseVector(rv, rs, rd)) =>
            val vec = Semigroup.plus[IndexedSeq[V]](lv, rv) match {
              case v: Vector[_] => v.asInstanceOf[Vector[V]]
              case notV => Vector(notV : _*)
            }
            fromVector(vec, ls)

          case _ if valueIsNonZero(left.sparseValue) =>
            fromVector(Vector(Semigroup.plus(toVector(left):IndexedSeq[V],
                                      toVector(right):IndexedSeq[V]):_*),
              left.sparseValue)
          case _ => // sparse is zero:
            fromMap(Semigroup.plus(toMap(left), toMap(right)),
              left.sparseValue,
              maxSize)
        }
      }
    }
  }
  private class AVMonoid[V:Monoid] extends AVSemigroup[V] with Monoid[AdaptiveVector[V]] {
    val zero = AdaptiveVector.fill[V](0)(Monoid.zero[V])
    override def isNonZero(v: AdaptiveVector[V]) = !isZero(v)

    def isZero(v: AdaptiveVector[V]) = (v.size == 0) || {
      val sparseAreZero = if(Monoid.isNonZero(v.sparseValue)) (v.denseCount == v.size) else true
      sparseAreZero &&
        v.denseIterator.forall { idxv => !Monoid.isNonZero(idxv._2) }
    }
  }
  private class AVGroup[V:Group] extends AVMonoid[V] with Group[AdaptiveVector[V]] {
    override def negate(v: AdaptiveVector[V]) =
      fromVector(toVector(v).map(Group.negate(_)), Group.negate(v.sparseValue))
  }

  implicit def semigroup[V:Semigroup]: Semigroup[AdaptiveVector[V]] = new AVSemigroup[V]
  implicit def monoid[V:Monoid]: Monoid[AdaptiveVector[V]] = new AVMonoid[V]
  implicit def group[V:Group]: Group[AdaptiveVector[V]] = new AVGroup[V]

  /*
   * Equality when considering only the dense values (so size doesn't matter)
   */
  def denseEquiv[V:Equiv]: Equiv[AdaptiveVector[V]] = Equiv.fromFunction[AdaptiveVector[V]] { (l, r) =>
    val (lit, rit) = (l.denseIterator, r.denseIterator)
    @tailrec
    def iteq: Boolean =
      (lit.hasNext, rit.hasNext) match {
        case (true, true) =>
          val (lnext, rnext) = (lit.next, rit.next)
          if(lnext._1 == rnext._1 && Equiv[V].equiv(lnext._2, rnext._2))
            iteq
          else
            false
        case (false, false) => true
        case _ => false
      }
    Equiv[V].equiv(l.sparseValue, r.sparseValue) && iteq
  }

  implicit def equiv[V:Equiv]: Equiv[AdaptiveVector[V]] =
    Equiv.fromFunction[AdaptiveVector[V]] { (l, r) =>
      (l.size == r.size) && (denseEquiv[V].equiv(l, r) ||
        toVector(l).view.zip(toVector(r)).forall { case (lv, rv) => Equiv[V].equiv(lv, rv) })
    }
}

/** An IndexedSeq that automatically switches representation between dense and sparse depending on sparsity
 * Should be an efficient representation for all sizes, and it should not be necessary to special case
 * immutable algebras based on the sparsity of the vectors.
 */
sealed trait AdaptiveVector[V] extends IndexedSeq[V] {
  def length = size
  def sparseValue: V
  /** How many items are not sparse */
  def denseCount: Int
  def size: Int
  def apply(idx: Int): V
  def updated(idx: Int, v: V): AdaptiveVector[V]
  /** Grow by adding count sparse values to the end */
  def extend(count: Int): AdaptiveVector[V]
  /** Iterator of indices and values of all non-sparse values */
  def denseIterator: Iterator[(Int,V)]
  /*
   * Note that IndexedSeq provides hashCode and equals that
   * work correctly based on length and apply.
   */
}

case class DenseVector[V](iseq: Vector[V], override val sparseValue: V, override val denseCount: Int)
  extends AdaptiveVector[V] {

  override def size = iseq.size
  def apply(idx: Int) = iseq(idx)
  def updated(idx: Int, v: V): AdaptiveVector[V] = {
    val oldIsSparse = if(iseq(idx) == sparseValue) 1 else 0
    val newIsSparse = if(v == sparseValue) 1 else 0
    val newCount = denseCount - newIsSparse + oldIsSparse
    if( denseCount < size * AdaptiveVector.THRESHOLD ) {
      // Go sparse
      SparseVector(AdaptiveVector.toMap(iseq, sparseValue), sparseValue, size)
    }
    else {
      DenseVector(iseq.updated(idx, v), sparseValue, newCount)
    }
  }
  def extend(cnt: Int) = {
    val newSize = size + cnt
    if(denseCount < newSize * AdaptiveVector.THRESHOLD) {
      // Go sparse
      SparseVector(AdaptiveVector.toMap(iseq, sparseValue), sparseValue, newSize)
    }
    else {
      // Stay dense
      DenseVector(iseq ++ Vector.fill(cnt)(sparseValue), sparseValue, denseCount)
    }
  }

  def denseIterator =
    iseq.view.zipWithIndex.filter { _._1 != sparseValue }.map { _.swap }.iterator
}

case class SparseVector[V](map: Map[Int, V], override val sparseValue: V, override val size: Int) extends
  AdaptiveVector[V] {

  def denseCount: Int = map.size
  def apply(idx: Int) = {
    require(idx >= 0 && idx < size, "Index out of range")
    map.getOrElse(idx, sparseValue)
  }
  def updated(idx: Int, v: V): AdaptiveVector[V] = {
    if(v == sparseValue) {
      SparseVector(map - idx, sparseValue, size)
    }
    else {
      val newM = map + (idx -> v)
      if(newM.size >= size * AdaptiveVector.THRESHOLD) {
        // Go dense:
        DenseVector(AdaptiveVector.toVector(newM, sparseValue, size), sparseValue, newM.size)
      }
      else {
        SparseVector(newM, sparseValue, size)
      }
    }
  }
  def extend(cnt: Int) = SparseVector(map, sparseValue, size + cnt)

  private lazy val sortedList = map.toList.sortBy { _._1 }
  def denseIterator = sortedList.iterator
}
