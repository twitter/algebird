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

import scala.annotation.tailrec

import algebra.{ BoundedSemilattice, Semilattice }

/**
 * Tracks the maximum wrapped instance of some ordered type `T`.
 *
 * [[Max]][T] is a [[Semigroup]] for all types `T`. If `T` has some
 * minimum element (`Long` has `Long.MinValue`, for example), then
 * [[Max]][T] is a [[Monoid]].
 *
 * @param get wrapped instance of `T`
 */
case class Max[@specialized(Int, Long, Float, Double) +T](get: T) {
  /**
   * If this instance wraps a larger `T` than `r`, returns this
   * instance, else returns `r`.
   *
   * @param r instance of `Max[U]` for comparison
   */
  def max[U >: T](r: Max[U])(implicit ord: Ordering[U]): Max[U] =
    Max.ordering.max(this, r)

  /**
   * Identical to [[max]].
   *
   * @param r instance of `Max[U]` for comparison
   */
  def +[U >: T](r: Max[U])(implicit ord: Ordering[U]): Max[U] = max(r)
}

/**
 * Provides a set of operations and typeclass instances needed to use
 * [[Max]] instances.
 */
object Max extends MaxInstances {
  /**
   * Returns an [[Aggregator]] that selects the maximum instance of an
   * ordered type `T` in the aggregated stream.
   */
  def aggregator[T](implicit ord: Ordering[T]): MaxAggregator[T] = MaxAggregator()(ord)

  /**
   * Returns a [[Semigroup]] instance with a `plus` implementation
   * that always returns the maximum `T` argument.
   */
  def maxSemigroup[T](implicit ord: Ordering[T]): Semigroup[T] with Semilattice[T] =
    new Semigroup[T] with Semilattice[T] {
      def plus(l: T, r: T): T = ord.max(l, r)
    }
}

private[algebird] sealed abstract class MaxInstances {
  implicit def equiv[T](implicit eq: Equiv[T]): Equiv[Max[T]] = Equiv.by(_.get)
  implicit def ordering[T: Ordering]: Ordering[Max[T]] = Ordering.by(_.get)

  /**
   * Returns a [[Monoid]] instance for [[Max]][T] that combines
   * instances using [[Max.max]] and uses `zero` for its identity.
   *
   * @param zero identity of the returned [[Monoid]] instance
   * @note `zero` must be `<=` every element of `T` for the returned instance to be lawful.
   */
  def monoid[T: Ordering](z: => T): Monoid[Max[T]] with BoundedSemilattice[Max[T]] =
    new Monoid[Max[T]] with BoundedSemilattice[Max[T]] {
      val zero = Max(z)
      val ord = implicitly[Ordering[T]]
      def plus(l: Max[T], r: Max[T]): Max[T] = if (ord.gteq(l.get, r.get)) l else r
    }

  /**
   * Returns a [[Semigroup]] instance for [[Max]][T]. The `plus`
   * implementation always returns the maximum `Max[T]` argument.
   */
  implicit def semigroup[T: Ordering]: Semigroup[Max[T]] with Semilattice[Max[T]] =
    // There's no need to override `sumOption`, since the default
    // implementation does no allocation other than the outer `Option`
    // and `plus` doesn't do any allocation.
    new Semigroup[Max[T]] with Semilattice[Max[T]] {
      val ord = implicitly[Ordering[T]]
      def plus(l: Max[T], r: Max[T]): Max[T] = if (ord.gteq(l.get, r.get)) l else r
    }

  /** [[Monoid]] for [[Max]][Int] with `zero == Int.MinValue` */
  implicit def intMonoid: Monoid[Max[Int]] with BoundedSemilattice[Max[Int]] = monoid(Int.MinValue)

  /** [[Monoid]] for [[Max]][Long] with `zero == Long.MinValue` */
  implicit def longMonoid: Monoid[Max[Long]] with BoundedSemilattice[Max[Long]] = monoid(Long.MinValue)

  /** [[Monoid]] for [[Max]][Double] with `zero == Double.MinValue` */
  implicit def doubleMonoid: Monoid[Max[Double]] with BoundedSemilattice[Max[Double]] = monoid(Double.MinValue)

  /** [[Monoid]] for [[Max]][Float] with `zero == Float.MinValue` */
  implicit def floatMonoid: Monoid[Max[Float]] with BoundedSemilattice[Max[Float]] = monoid(Float.MinValue)

  /** [[Monoid]] for [[Max]][String] with `zero == ""` */
  implicit def stringMonoid: Monoid[Max[String]] = monoid("")

  /**
   * Returns a [[Monoid]] instance for `Max[List[T]]` that compares
   * lists first by length and then element-wise by `T`, and returns
   * the maximum value.
   */
  implicit def listMonoid[T: Ordering]: Monoid[Max[List[T]]] with BoundedSemilattice[Max[List[T]]] = monoid[List[T]](Nil)(
    new Ordering[List[T]] {
      @tailrec
      final override def compare(left: List[T], right: List[T]): Int = {
        (left, right) match {
          case (Nil, Nil) => 0
          case (Nil, _) => -1
          case (_, Nil) => 1
          case (lh :: lt, rh :: rt) =>
            val c = Ordering[T].compare(lh, rh)
            if (c == 0) compare(lt, rt) else c
        }
      }
    })

  // TODO: Replace with
  // cast.kernel.instances.StaticMethods.iteratorCompare when we
  // merge with cats.
  private def iteratorCompare[T](xs: Iterator[T], ys: Iterator[T])(implicit ord: Ordering[T]): Int = {
    while (true) {
      if (xs.hasNext) {
        if (ys.hasNext) {
          val x = xs.next
          val y = ys.next
          val cmp = ord.compare(x, y)
          if (cmp != 0) return cmp
        } else {
          return 1
        }
      } else {
        return if (ys.hasNext) -1 else 0
      }
    }
    0
  }

  /**
   * Returns a [[Monoid]] instance for `Max[Vector[T]]` that compares
   * lists first by length and then element-wise by `T`, and returns
   * the maximum value.
   */
  implicit def vectorMonoid[T: Ordering]: Monoid[Max[Vector[T]]] with BoundedSemilattice[Max[Vector[T]]] =
    monoid[Vector[T]](Vector.empty[T])(
      new Ordering[Vector[T]] {
        def compare(l: Vector[T], r: Vector[T]): Int = {
          if (l eq r) 0
          else iteratorCompare(l.iterator, r.iterator)
        }
      })

  /**
   * Returns a [[Monoid]] instance for `Max[Stream[T]]` that compares
   * lists first by length and then element-wise by `T`, and returns
   * the maximum value.
   */
  implicit def streamMonoid[T: Ordering]: Monoid[Max[Stream[T]]] with BoundedSemilattice[Max[Stream[T]]] =
    monoid[Stream[T]](Stream.empty[T])(
      new Ordering[Stream[T]] {
        def compare(l: Stream[T], r: Stream[T]): Int = {
          if (l eq r) 0
          else iteratorCompare(l.iterator, r.iterator)
        }
      })
}

/**
 * [[Aggregator]] that selects the maximum instance of `T` in the
 * aggregated stream.
 */
case class MaxAggregator[T](implicit ord: Ordering[T]) extends Aggregator[T, T, T] {
  def prepare(v: T) = v
  val semigroup = Max.maxSemigroup[T]
  def present(v: T) = v
}
