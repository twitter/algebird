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
 * A convenience class that composes AdaptiveVector[AdaptiveVector[V]] with
 * some useful functions.
 */

object AdaptiveMatrix {
  def fill[V](rows: Int, columns: Int)(fill: V): AdaptiveMatrix[V] = {
    AdaptiveMatrix[V](AdaptiveVector.fill(rows)(AdaptiveVector.fill[V](columns)(fill)))
  }

  /**
   * Use recursive AdaptiveVector monoid.
   */
  implicit def monoid[V:Monoid]: Monoid[AdaptiveMatrix[V]] = new Monoid[AdaptiveMatrix[V]] {
    // Scala 2.10.0 is more strict with recursive implicit resolution, so hint
    // it with the inner monoid.
    private implicit val innerMonoid: Monoid[AdaptiveVector[V]] = AdaptiveVector.monoid[V]
    private val matrixMonoid = AdaptiveVector.monoid[AdaptiveVector[V]]

    override def zero: AdaptiveMatrix[V] = AdaptiveMatrix[V](matrixMonoid.zero)
    override def plus(left: AdaptiveMatrix[V], right: AdaptiveMatrix[V]): AdaptiveMatrix[V] = {
      AdaptiveMatrix[V](matrixMonoid.plus(left.rowsByColumns, right.rowsByColumns))
    }
  }
}

case class AdaptiveMatrix[V](rowsByColumns: AdaptiveVector[AdaptiveVector[V]]) {
  /** Rows are the outer vectors, and columns are the inner vectors. */
  def rows: Int = rowsByColumns.size
  def columns: Int = rowsByColumns(0).size

  def getValue(position: (Int, Int)): V = rowsByColumns(position._1)(position._2)

  def updated(position: (Int, Int), value: V): AdaptiveMatrix[V] = {
    val (row, col) = position
    AdaptiveMatrix[V](rowsByColumns.updated(row, rowsByColumns(row).updated(col, value)))
  }
}

