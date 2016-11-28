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

// To use the MaxSemigroup wrap your item in Max
case class Max[@specialized(Int, Long, Float, Double) +T](get: T) {
  def max[U >: T](r: Max[U])(implicit ord: Ordering[U]): Max[U] =
    Max.ordering.max(this, r)
  def +[U >: T](r: Max[U])(implicit ord: Ordering[U]): Max[U] = max(r)
}

object Max extends MaxInstances {
  def aggregator[T](implicit ord: Ordering[T]): MaxAggregator[T] = MaxAggregator()(ord)

  // TODO: Note that this is a semilattice, for when we merge
  // typelevel/algebra.
  def maxSemigroup[T](implicit ord: Ordering[T]): Semigroup[T] =
    Semigroup.from { (l: T, r: T) => ord.max(l, r) }
}

private[algebird] sealed abstract class MaxInstances {
  implicit def equiv[T](implicit eq: Equiv[T]): Equiv[Max[T]] = Equiv.by(_.get)

  private[this] def plus[T](implicit ord: Ordering[T]) = {
    (l: Max[T], r: Max[T]) => if (ord.gteq(l.get, r.get)) l else r
  }

  // Zero should have the property that it <= all T
  def monoid[T: Ordering](zero: => T): Monoid[Max[T]] = Monoid.from(Max(zero))(plus)

  // There's no need to override `sumOption`, since the default
  // implementation does no allocation other than the outer `Option`
  // and `plus` doesn't do any allocation.
  implicit def semigroup[T: Ordering]: Semigroup[Max[T]] = Semigroup.from(plus)

  implicit def ordering[T: Ordering]: Ordering[Max[T]] = Ordering.by(_.get)

  implicit def intMonoid: Monoid[Max[Int]] = monoid(Int.MinValue)
  implicit def longMonoid: Monoid[Max[Long]] = monoid(Long.MinValue)
  implicit def doubleMonoid: Monoid[Max[Double]] = monoid(Double.MinValue)
  implicit def floatMonoid: Monoid[Max[Float]] = monoid(Float.MinValue)

  // These have a lower bound, but not an upperbound, so the Max forms a monoid:
  implicit def stringMonoid: Monoid[Max[String]] = monoid("")

  implicit def listMonoid[T: Ordering]: Monoid[Max[List[T]]] = monoid[List[T]](Nil)(
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
  def iteratorCompare[T](xs: Iterator[T], ys: Iterator[T])(implicit ord: Ordering[T]): Int = {
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

  implicit def vectorMonoid[T: Ordering]: Monoid[Max[Vector[T]]] =
    monoid[Vector[T]](Vector.empty[T])(
      new Ordering[Vector[T]] {
        def compare(l: Vector[T], r: Vector[T]): Int = {
          if (l eq r) 0
          else iteratorCompare(l.iterator, r.iterator)
        }
      })

  implicit def streamMonoid[T: Ordering]: Monoid[Max[Stream[T]]] =
    monoid[Stream[T]](Stream.empty[T])(
      new Ordering[Stream[T]] {
        def compare(l: Stream[T], r: Stream[T]): Int = {
          if (l eq r) 0
          else iteratorCompare(l.iterator, r.iterator)
        }
      })
}

case class MaxAggregator[T](implicit ord: Ordering[T]) extends Aggregator[T, T, T] {
  def prepare(v: T) = v
  val semigroup = Max.maxSemigroup[T]
  def present(v: T) = v
}
