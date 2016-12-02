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

/**
 * Tracks the minimum wrapped instance of some ordered type `T`.
 *
 * [[Min]][T] is a [[Semigroup]] for all types `T`. If `T` has some
 * maximum element (`Long` has `Long.MaxValue`, for example), then
 * [[Min]][T] is a [[Monoid]].
 *
 * @param get wrapped instance of `T`
 */
case class Min[@specialized(Int, Long, Float, Double) +T](get: T) {
  /**
   * If this instance wraps a smaller `T` than `r`, returns this
   * instance, else returns `r`.
   *
   * @param r instance of `Min[U]` for comparison
   */
  def min[U >: T](r: Min[U])(implicit ord: Ordering[U]): Min[U] =
    Min.ordering.min(this, r)

  /**
   * Identical to [[min]].
   *
   * @param r instance of `Min[U]` for comparison
   */
  def +[U >: T](r: Min[U])(implicit ord: Ordering[U]): Min[U] = min(r)
}

/**
 * Provides a set of operations and typeclass instances needed to use
 * [[Min]] instances.
 */
object Min extends MinInstances {
  /**
   * Returns an [[Aggregator]] that selects the minimum instance of an
   * ordered type `T` in the aggregated stream.
   */
  def aggregator[T](implicit ord: Ordering[T]): MinAggregator[T] = MinAggregator()(ord)

  /**
   * Returns a [[Semigroup]] instance with a `plus` implementation
   * that always returns the minimum `T` argument.
   */
  def minSemigroup[T](implicit ord: Ordering[T]): Semigroup[T] =
    Semigroup.from { (l: T, r: T) => ord.min(l, r) }
}

private[algebird] sealed abstract class MinInstances {
  implicit def equiv[T](implicit eq: Equiv[T]): Equiv[Min[T]] = Equiv.by(_.get)
  implicit def ordering[T: Ordering]: Ordering[Min[T]] = Ordering.by(_.get)

  private[this] def plus[T](implicit ord: Ordering[T]) = {
    (l: Min[T], r: Min[T]) => if (ord.lteq(l.get, r.get)) l else r
  }

  /**
   * Returns a [[Monoid]] instance for [[Min]][T] that combines
   * instances using [[Min.min]] and uses `zero` for its identity.
   *
   * @param zero identity of the returned [[Monoid]] instance
   * @note `zero` must be `>=` every element of `T` for the returned instance to be lawful.
   */
  def monoid[T: Ordering](zero: => T): Monoid[Min[T]] = Monoid.from(Min(zero))(plus)

  /**
   * Returns a [[Semigroup]] instance for [[Min]][T]. The `plus`
   * implementation always returns the minimum `Min[T]` argument.
   */
  implicit def semigroup[T: Ordering]: Semigroup[Min[T]] = Semigroup.from(plus)

  /** [[Monoid]] for [[Min]][Int] with `zero == Int.MaxValue` */
  implicit def intMonoid: Monoid[Min[Int]] = monoid(Int.MaxValue)

  /** [[Monoid]] for [[Min]][Long] with `zero == Long.MaxValue` */
  implicit def longMonoid: Monoid[Min[Long]] = monoid(Long.MaxValue)

  /** [[Monoid]] for [[Min]][Double] with `zero == Double.MaxValue` */
  implicit def doubleMonoid: Monoid[Min[Double]] = monoid(Double.MaxValue)

  /** [[Monoid]] for [[Min]][Float] with `zero == Float.MaxValue` */
  implicit def floatMonoid: Monoid[Min[Float]] = monoid(Float.MaxValue)
}

/**
 * [[Aggregator]] that selects the minimum instance of `T` in the
 * aggregated stream.
 */
case class MinAggregator[T](implicit ord: Ordering[T]) extends Aggregator[T, T, T] {
  def prepare(v: T) = v
  val semigroup = Min.minSemigroup[T]
  def present(v: T) = v
}
