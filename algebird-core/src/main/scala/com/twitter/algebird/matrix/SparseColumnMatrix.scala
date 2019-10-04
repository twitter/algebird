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
import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import com.twitter.algebird.{AdaptiveVector, Monoid}

object SparseColumnMatrix {
  def fromSeqMap[V: Monoid](cols: Int, data: IndexedSeq[MMap[Int, V]]): SparseColumnMatrix[V] = {
    val monoidZero = implicitly[Monoid[V]].zero
    SparseColumnMatrix(data.map { mm =>
      AdaptiveVector.fromMap(mm.toMap, monoidZero, cols)
    }.toIndexedSeq)
  }
}

case class SparseColumnMatrix[V: Monoid](rowsByColumns: IndexedSeq[AdaptiveVector[V]])
    extends AdaptiveMatrix[V] {

  /** Row is the outer Seq, the columns are the inner vectors. */
  val valueMonoid: Monoid[V] = implicitly[Monoid[V]]

  override def rows: Int = rowsByColumns.size

  override def cols: Int = rowsByColumns(0).size

  override def getValue(position: (Int, Int)): V =
    rowsByColumns(position._1)(position._2)

  override def updated(position: (Int, Int), value: V): SparseColumnMatrix[V] = {
    val (row, col) = position
    SparseColumnMatrix[V](rowsByColumns.updated(row, rowsByColumns(row).updated(col, value)))
  }

  override def updateInto(buffer: ArrayBuffer[V]): Unit = {
    val lcols = cols
    var row = 0
    while (row < rows) {
      val iter = rowsByColumns(row).denseIterator
      while (iter.hasNext) {
        val (col, value) = iter.next
        val indx = row * lcols + col
        buffer(indx) = valueMonoid.plus(buffer(indx), value)
      }
      row += 1
    }
  }

  def toDense: DenseMatrix[V] = {
    val buf = ArrayBuffer.fill(size)(valueMonoid.zero)
    updateInto(buf)
    DenseMatrix(rows, cols, buf)
  }

  override def toString: String = {
    val builder = new scala.collection.mutable.StringBuilder()
    builder ++= "Row: %d, Cols: %d. Dense elements:\n".format(rows, cols)
    rowsByColumns.foreach { v =>
      builder ++= v.denseIterator.toList.toString
      builder ++= "\n"
    }
    builder.toString
  }
}
