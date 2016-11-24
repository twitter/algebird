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

// To use the MinSemigroup wrap your item in a Min object
case class Min[@specialized(Int, Long, Float, Double) +T](get: T)

object Min extends MinInstances {
  def aggregator[T](implicit ord: Ordering[T]): MinAggregator[T] = MinAggregator()(ord)
}

private[algebird] sealed abstract class MinInstances {
  implicit def equiv[T](implicit eq: Equiv[T]): Equiv[Min[T]] = Equiv.by(_.get)

  // Zero should have the property that it >= all T
  def monoid[T](zero: => T)(implicit ord: Ordering[T]): Monoid[Min[T]] =
    Monoid.from(Min(zero)) { (l, r) => if (ord.lteq(l.get, r.get)) l else r }

  implicit def semigroup[T](implicit ord: Ordering[T]): Semigroup[Min[T]] =
    Semigroup.from[Min[T]] { (l, r) => if (ord.lteq(l.get, r.get)) l else r }

  implicit def ordering[T](implicit ord: Ordering[T]): Ordering[Min[T]] =
    Ordering.by(_.get)

  implicit def intMonoid: Monoid[Min[Int]] = monoid(Int.MaxValue)
  implicit def longMonoid: Monoid[Min[Long]] = monoid(Long.MaxValue)
  implicit def doubleMonoid: Monoid[Min[Double]] = monoid(Double.MaxValue)
  implicit def floatMonoid: Monoid[Min[Float]] = monoid(Float.MaxValue)
}

case class MinAggregator[T](implicit ord: Ordering[T]) extends Aggregator[T, T, T] {
  def prepare(v: T) = v
  val semigroup = Semigroup.from { (l: T, r: T) => ord.min(l, r) }
  def present(v: T) = v
}
