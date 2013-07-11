/*
 Copyright 2013 Twitter, Inc.

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

import com.twitter.algebird.Monoid

// TODO this is clearly more general than summingbird, and should be extended to be a ring (add union, etc...)

/** Represents a single interval on a T with an Ordering
 */
sealed trait Interval[T] extends (T => Boolean) with java.io.Serializable {
  def contains(t: T): Boolean

  def intersect(that: Interval[T]): Interval[T]
  def apply(t: T) = contains(t)
  def &&(that: Interval[T]) = intersect(that)

  /** Map the Interval with a non-decreasing function.
   * If you use a non-monotonic function (like x^2)
   * then the result is meaningless.
   * TODO: It might be good to have types for these properties in algebird.
   */
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U]
}

case class Universe[T]() extends Interval[T] {
  def contains(t: T): Boolean = true
  def intersect(that: Interval[T]): Interval[T] = that
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = Universe()
}

case class Empty[T]() extends Interval[T] {
  def contains(t: T): Boolean = false
  def intersect(that: Interval[T]): Interval[T] = this
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = Empty()
}

object Interval extends java.io.Serializable {
  implicit def monoid[T]: Monoid[Interval[T]] = Monoid.from[Interval[T]](Universe[T]()) { _ && _ }

  def leftClosedRightOpen[T:Ordering](lower: T, upper: T): Interval[T] =
    InclusiveLower(lower) && ExclusiveUpper(upper)
}

// Marker traits to keep lower on the left in Intersection
sealed trait Lower[T] extends Interval[T]
sealed trait Upper[T] extends Interval[T]

case class InclusiveLower[T](lower: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Lower[T] {
  def contains(t: T): Boolean = ordering.lteq(lower, t)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Universe() => this
    case Empty() => that
    case ub@InclusiveUpper(upper) =>
      if (ub.ordering.lt(upper, lower)) Empty() else Intersection[T](this, ub)
    case ub@ExclusiveUpper(upper) =>
      if (ub.ordering.lteq(upper, lower)) Empty() else Intersection[T](this, ub)
    case lb@InclusiveLower(thatlb) => if (lb.ordering.gt(lower, thatlb)) this else that
    case lb@ExclusiveLower(thatlb) => if (lb.ordering.gt(lower, thatlb)) this else that
    case Intersection(thatL, thatU) => (this && thatL) && thatU
  }
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = InclusiveLower(fn(lower))
}
case class ExclusiveLower[T](lower: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Lower[T] {
  def contains(t: T): Boolean = ordering.lt(lower, t)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Universe() => this
    case Empty() => that
    case ub@InclusiveUpper(upper) =>
      if (ub.ordering.lteq(upper, lower)) Empty() else Intersection[T](this, ub)
    case ub@ExclusiveUpper(upper) =>
      if (ub.ordering.lteq(upper, lower)) Empty() else Intersection[T](this, ub)
    case lb@InclusiveLower(thatlb) => if (lb.ordering.gteq(lower, thatlb)) this else that
    case lb@ExclusiveLower(thatlb) => if (lb.ordering.gteq(lower, thatlb)) this else that
    case Intersection(thatL, thatU) => (this && thatL) && thatU
  }
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = ExclusiveLower(fn(lower))
}
case class InclusiveUpper[T](upper: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Upper[T] {
  def contains(t: T): Boolean = ordering.lteq(t, upper)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Universe() => this
    case Empty() => that
    case lb@InclusiveLower(lower) =>
      if (lb.ordering.lt(upper, lower)) Empty() else Intersection[T](lb, this)
    case lb@ExclusiveLower(lower) =>
      if (lb.ordering.lteq(upper, lower)) Empty() else Intersection[T](lb, this)
    case ub@InclusiveUpper(thatub) =>
      if (ub.ordering.lt(upper, thatub)) this else that
    case ub@ExclusiveUpper(thatub) =>
      if (ub.ordering.lt(upper, thatub)) this else that
    case Intersection(thatL, thatU) => thatL && (this && thatU)
  }
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = InclusiveUpper(fn(upper))
}
case class ExclusiveUpper[T](upper: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Upper[T] {
  def contains(t: T): Boolean = ordering.lt(t, upper)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Universe() => this
    case Empty() => that
    case lb@InclusiveLower(lower) =>
      if (lb.ordering.lteq(upper, lower)) Empty() else Intersection[T](lb, this)
    case lb@ExclusiveLower(lower) =>
      if (lb.ordering.lteq(upper, lower)) Empty() else Intersection[T](lb, this)
    case ub@InclusiveUpper(thatub) =>
      if (ub.ordering.lteq(upper, thatub)) this else that
    case ub@ExclusiveUpper(thatub) =>
      if (ub.ordering.lteq(upper, thatub)) this else that
    case Intersection(thatL, thatU) => thatL && (this && thatU)
  }
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = ExclusiveUpper(fn(upper))
}

case class Intersection[T](lower: Lower[T], upper: Upper[T]) extends Interval[T] {
  def contains(t: T): Boolean = lower.contains(t) && upper.contains(t)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Intersection(thatL, thatU) => (lower && thatL) && (upper && thatU)
    case _ => (lower && that) && (upper && that)
  }
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] =
    lower.mapNonDecreasing(fn) && upper.mapNonDecreasing(fn)
}
