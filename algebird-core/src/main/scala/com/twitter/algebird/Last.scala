/*
Copyright 2016 Twitter, Inc.

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

import algebra.Band

/**
 * Tracks the "most recent", or last, wrapped instance of `T` by the
 * order in which items are seen.
 *
 * @param get wrapped instance of `T`
 */
case class Last[@specialized(Int, Long, Float, Double) +T](get: T) {

  /**
   * Returns the argument `r`, always.
   *
   * @param r returned of `Last[U]`
   */
  def +[U >: T](r: Last[U]): Last[U] = r
}

/**
 * Provides a set of operations and typeclass instances needed to use
 * [[Last]] instances.
 */
object Last extends LastInstances {

  /**
   * Returns an [[Aggregator]] that selects the last instance of `T`
   * in the aggregated stream.
   */
  def aggregator[T]: LastAggregator[T] = LastAggregator()
}

private[algebird] sealed abstract class LastInstances {

  /**
   * Returns a [[Semigroup]] instance with a `plus` implementation
   * that always returns the last (ie, the right) `T` argument.
   */
  def lastSemigroup[T]: Semigroup[T] with Band[T] =
    new Semigroup[T] with Band[T] {
      override def plus(l: T, r: T): T = r
      override def sumOption(ts: TraversableOnce[T]): Option[T] = {
        var res: Option[T] = None
        ts.foreach(t => res = Some(t))
        res
      }
    }

  /**
   * Returns a [[Semigroup]] instance for [[Last]][T]. The `plus`
   * implementation always returns the last (ie, the right) `Last[T]`
   * argument.
   */
  implicit def semigroup[T]: Semigroup[Last[T]] with Band[Last[T]] =
    lastSemigroup[Last[T]]
}

/**
 * [[Aggregator]] that selects the last instance of `T` in the
 * aggregated stream.
 */
case class LastAggregator[T]() extends Aggregator[T, T, T] {
  override def prepare(v: T): T = v
  override val semigroup: Semigroup[T] = Last.lastSemigroup[T]
  override def present(v: T): T = v
}
