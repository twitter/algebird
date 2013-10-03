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

import scala.annotation.tailrec

// To use the MaxSemigroup wrap your item in a Max object
case class Max[@specialized(Int,Long,Float,Double) +T](get: T)

object Max {
  implicit def semigroup[T](implicit ord:Ordering[T]) =
    Semigroup.from[Max[T]] { (l,r) => if(ord.gteq(l.get, r.get)) l else r }

  // Zero should have the property that it <= all T
  def monoid[T](zero: => T)(implicit ord: Ordering[T]): Monoid[Max[T]] =
     Monoid.from(Max(zero)) { (l,r) => if(ord.gteq(l.get, r.get)) l else r }

  def aggregator[T](implicit ord:Ordering[T]): MaxAggregator[T] = MaxAggregator()(ord)

  implicit def intMonoid: Monoid[Max[Int]] = monoid(Int.MinValue)
  implicit def longMonoid: Monoid[Max[Long]] = monoid(Long.MinValue)
  implicit def doubleMonoid: Monoid[Max[Double]] = monoid(Double.MinValue)
  implicit def floatMonoid: Monoid[Max[Float]] = monoid(Float.MinValue)

  // These have a lower bound, but not an upperbound, so the Max forms a monoid:
  implicit def stringMonoid: Monoid[Max[String]] = monoid("")
  implicit def listMonoid[T:Ordering]: Monoid[Max[List[T]]] = monoid[List[T]](Nil)(new Ordering[List[T]] {
    @tailrec
    final override def compare(left: List[T], right: List[T]): Int = {
      (left, right) match {
        case (Nil, Nil) => 0
        case (Nil, _) => -1
        case (_, Nil) => 1
        case (lh::lt, rh::rt) =>
          val c = Ordering[T].compare(lh, rh)
          if(c == 0) compare(lt, rt) else c
      }
    }
  })
}

// To use the MinSemigroup wrap your item in a Min object
case class Min[@specialized(Int,Long,Float,Double) +T](get: T)

object Min {
  implicit def semigroup[T](implicit ord:Ordering[T]) =
    Semigroup.from[Min[T]] { (l,r) => if(ord.lteq(l.get, r.get)) l else r }

  // Zero should have the property that it >= all T
  def monoid[T](zero: => T)(implicit ord: Ordering[T]): Monoid[Min[T]] =
     Monoid.from(Min(zero)) { (l,r) => if(ord.lteq(l.get, r.get)) l else r }

  def aggregator[T](implicit ord:Ordering[T]): MinAggregator[T] = MinAggregator()(ord)

  implicit def intMonoid: Monoid[Min[Int]] = monoid(Int.MaxValue)
  implicit def longMonoid: Monoid[Min[Long]] = monoid(Long.MaxValue)
  implicit def doubleMonoid: Monoid[Min[Double]] = monoid(Double.MaxValue)
  implicit def floatMonoid: Monoid[Min[Float]] = monoid(Float.MaxValue)
}

// Not ordered by type, but ordered by order in which we see them:

case class First[@specialized(Int,Long,Float,Double) +T](get: T)
object First {
  implicit def semigroup[T] = Semigroup.from[First[T]] { (l,r) => l }
}

case class Last[@specialized(Int,Long,Float,Double) +T](get: T)
object Last {
  implicit def semigroup[T] = Semigroup.from[Last[T]] { (l,r) => r }
}

case class MinAggregator[T](implicit ord: Ordering[T]) extends Aggregator[T, T, T] {
  def prepare(v: T) = v
  def reduce(l: T, r: T) = ord.min(l , r)
  def present(v: T) = v
}

case class MaxAggregator[T](implicit ord: Ordering[T]) extends Aggregator[T, T, T] {
  def prepare(v: T) = v
  def reduce(l: T, r: T) = ord.max(l , r)
  def present(v: T) = v
}
