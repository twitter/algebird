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

package com.twitter.algebird.matrix

import scala.collection.mutable.{ ArrayBuffer, Map => MMap }
import com.twitter.algebird.{ AdaptiveVector, HasAdditionOperatorAndZero }

/**
 * A Matrix structure that is designed to hide moving between sparse and dense representations
 * Initial support here is focused on a dense row count with a sparse set of columns
 */

abstract class AdaptiveMatrix[V: HasAdditionOperatorAndZero] extends Serializable {
  def rows: Int
  def cols: Int
  def size = rows * cols

  def getValue(position: (Int, Int)): V

  def updateInto(buffer: ArrayBuffer[V]): Unit

  def updated(position: (Int, Int), value: V): AdaptiveMatrix[V]
}

object AdaptiveMatrix {
  def zero[V: HasAdditionOperatorAndZero](rows: Int, cols: Int) = fill(rows, cols)(implicitly[HasAdditionOperatorAndZero[V]].zero)

  def fill[V: HasAdditionOperatorAndZero](rows: Int, cols: Int)(fill: V): AdaptiveMatrix[V] = {
    SparseColumnMatrix(Vector.fill(rows)(AdaptiveVector.fill[V](cols)(fill)))
  }

  def empty[V: HasAdditionOperatorAndZero](): AdaptiveMatrix[V] = {
    SparseColumnMatrix(IndexedSeq[AdaptiveVector[V]]())
  }

  // The adaptive monoid to swap between sparse modes.
  implicit def monoid[V: HasAdditionOperatorAndZero]: HasAdditionOperatorAndZero[AdaptiveMatrix[V]] = new HasAdditionOperatorAndZero[AdaptiveMatrix[V]] {
    private[this] final val innerZero = implicitly[HasAdditionOperatorAndZero[V]].zero

    override def zero: AdaptiveMatrix[V] = SparseColumnMatrix[V](IndexedSeq[AdaptiveVector[V]]())

    override def plus(a: AdaptiveMatrix[V], b: AdaptiveMatrix[V]) = sumOption(List(a, b)).get

    private def denseInsert(rows: Int, cols: Int, buff: ArrayBuffer[V], remainder: Iterator[AdaptiveMatrix[V]]): Option[AdaptiveMatrix[V]] = {
      remainder.foreach(_.updateInto(buff))
      Some(DenseMatrix(rows, cols, buff))
    }

    private def denseUpdate(current: AdaptiveMatrix[V], remainder: Iterator[AdaptiveMatrix[V]]): Option[AdaptiveMatrix[V]] = {
      val rows = current.rows
      val cols = current.cols
      val buffer = ArrayBuffer.fill(rows * cols)(innerZero)
      current.updateInto(buffer)
      denseInsert(rows, cols, buffer, remainder)
    }

    private def sparseUpdate(storage: IndexedSeq[MMap[Int, V]], other: SparseColumnMatrix[V]) = {
      other.rowsByColumns.zipWithIndex.foreach {
        case (contents, indx) =>
          val curMap: MMap[Int, V] = storage(indx)
          AdaptiveVector.toMap(contents).foreach {
            case (col, value) =>
              curMap.update(col, HasAdditionOperatorAndZero.plus(value, curMap.getOrElse(col, innerZero)))
          }
      }
    }

    private def goDense(rows: Int, cols: Int, storage: IndexedSeq[MMap[Int, V]], remainder: Iterator[AdaptiveMatrix[V]]): Option[AdaptiveMatrix[V]] = {
      val buffer = ArrayBuffer.fill(rows * cols)(innerZero)
      var row = 0
      val iter = storage.iterator
      while (iter.hasNext) {
        val curRow = iter.next
        curRow.foreach {
          case (col, value) =>
            buffer(row * cols + col) = value
        }
        row += 1
      }
      denseInsert(rows, cols, buffer, remainder)
    }

    override def sumOption(items: TraversableOnce[AdaptiveMatrix[V]]): Option[AdaptiveMatrix[V]] =
      if (items.isEmpty) {
        None
      } else {
        val iter = items.toIterator.buffered
        val rows = iter.head.rows
        val cols = iter.head.cols
        val sparseStorage = (0 until rows).map{ _ => MMap[Int, V]() }.toIndexedSeq

        while (iter.hasNext) {
          val current = iter.next
          current match {
            case d @ DenseMatrix(_, _, _) => return denseUpdate(d, iter)
            case s @ SparseColumnMatrix(_) =>
              sparseUpdate(sparseStorage, s)
              if (sparseStorage(0).size > current.cols / 4) {
                return goDense(rows, cols, sparseStorage, iter)
              }
          }
        }

        // Need to still be sparse to reach here, so must unpack the MMap to be used again.
        Some(SparseColumnMatrix.fromSeqMap(cols, sparseStorage))
      }
  }
}

