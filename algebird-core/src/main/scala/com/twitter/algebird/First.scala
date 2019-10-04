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
 * Tracks the "least recent", or earliest, wrapped instance of `T` by
 * the order in which items are seen.
 *
 * @param get wrapped instance of `T`
 */
case class First[@specialized(Int, Long, Float, Double) +T](get: T) {

  /**
   * Returns this instance, always.
   *
   * @param r ignored instance of `First[U]`
   */
  def +[U >: T](r: First[U]): First[T] = {
    val _ = r //suppressing unused `r`
    this
  }
}

/**
 * Provides a set of operations and typeclass instances needed to use
 * [[First]] instances.
 */
object First extends FirstInstances {

  /**
   * Returns an [[Aggregator]] that selects the first instance of `T`
   * in the aggregated stream.
   */
  def aggregator[T]: FirstAggregator[T] = FirstAggregator()
}

private[algebird] sealed abstract class FirstInstances {

  /**
   * Returns a [[Semigroup]] instance with a `plus` implementation
   * that always returns the first (ie, the left) `T` argument.
   *
   * This semigroup's `sumOption` is efficient; it only selects the
   * head of the `TraversableOnce` instance, leaving the rest
   * untouched.
   */
  def firstSemigroup[T]: Semigroup[T] with Band[T] =
    new Semigroup[T] with Band[T] {
      override def plus(l: T, r: T): T = l

      override def sumOption(iter: TraversableOnce[T]): Option[T] =
        if (iter.isEmpty) None else Some(iter.toIterator.next)
    }

  /**
   * Returns a [[Semigroup]] instance for [[First]][T]. The `plus`
   * implementation always returns the first (ie, the left) `First[T]`
   * argument.
   */
  implicit def semigroup[T]: Semigroup[First[T]] with Band[First[T]] =
    firstSemigroup[First[T]]
}

/**
 * [[Aggregator]] that selects the first instance of `T` in the
 * aggregated stream.
 */
case class FirstAggregator[T]() extends Aggregator[T, T, T] {
  override def prepare(v: T): T = v
  override val semigroup: Semigroup[T] = First.firstSemigroup[T]
  override def present(v: T): T = v
}
