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

import com.twitter.algebird.Monoid

case class DenseMatrix[V: Monoid](rows: Int, cols: Int, rowsByColumns: IndexedSeq[V]) extends AdaptiveMatrix[V] {
  val valueMonoid = implicitly[Monoid[V]]

  private[this] def tupToIndex(position: (Int, Int)) = position._1 * cols + position._2

  override def getValue(position: (Int, Int)): V = rowsByColumns(tupToIndex(position))

  override def updated(position: (Int, Int), value: V): DenseMatrix[V] =
    DenseMatrix[V](rows, cols, rowsByColumns.updated(tupToIndex(position), value))

  override def updateInto(buffer: ArrayBuffer[V]) {
    var indx = 0
    val lsize = size
    while (indx < lsize) {
      buffer(indx) = valueMonoid.plus(buffer(indx), rowsByColumns(indx))
      indx += 1
    }
  }

}

