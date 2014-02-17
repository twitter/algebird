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

// TODO this is clearly more general than summingbird, and should be extended to be a ring (add union, etc...)

/** Represents a single interval on a T with an Ordering
 * TODO remove T => Boolean. it ruins toString and doesn't help anything
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
  def leftOpenRightClosed[T:Ordering](lower: T, upper: T): Interval[T] =
    ExclusiveLower(lower) && InclusiveUpper(upper)
}

// Marker traits to keep lower on the left in Intersection
sealed trait Lower[T] extends Interval[T] {
  /**
   * The smallest value that is contained here
   * This is an Option, because of cases like ExclusiveLower(Int.MaxValue)
   * which are pathological and equivalent to Empty
   */
  def least(implicit s: Successible[T]): Option[T]
  /** Iterates all the items in this Lower[T] from lowest to highest
   */
  def toIterable(implicit s: Successible[T]): Iterable[T] =
    least match {
      case Some(l) => s.iterateNext(l)
      case None => Iterable.empty
    }
}
sealed trait Upper[T] extends Interval[T] {
  /**
   * The smallest value that is contained here
   * This is an Option, because of cases like ExclusiveUpper(Int.MinValue),
   * which are pathological and equivalent to Empty
   */
  def greatest(implicit p: Predecessible[T]): Option[T]
  /** Iterates all the items in this Upper[T] from highest to lowest
   */
  def toIterable(implicit p: Predecessible[T]): Iterable[T] =
    greatest match {
      case Some(g) => p.iteratePrev(g)
      case None => Iterable.empty
    }
}

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
  def least(implicit s: Successible[T]): Option[T] = Some(lower)
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
  def least(implicit s: Successible[T]): Option[T] = s.next(lower)
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = ExclusiveLower(fn(lower))
}
case class InclusiveUpper[T](upper: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Upper[T] {
  def contains(t: T): Boolean = ordering.lteq(t, upper)
  def greatest(implicit p: Predecessible[T]): Option[T] = Some(upper)
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
  def greatest(implicit p: Predecessible[T]): Option[T] = p.prev(upper)
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
    case Universe() => this
    case Empty() => that
    case lb@InclusiveLower(_) => (lb && lower) && upper
    case lb@ExclusiveLower(_) => (lb && lower) && upper
    case ub@InclusiveUpper(_) => lower && (ub && upper)
    case ub@ExclusiveUpper(_) => lower && (ub && upper)
    case Intersection(thatL, thatU) => (lower && thatL) && (upper && thatU)
  }
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] =
    lower.mapNonDecreasing(fn) && upper.mapNonDecreasing(fn)

  /** Goes from lowest to highest for all items
   * that are contained in this Intersection
   */
  def leastToGreatest(implicit s: Successible[T]): Iterable[T] = {
    val self = this
    new AbstractIterable[T] {
      // we have to do this because the normal takeWhile causes OOM on big intervals
      def iterator = lower.toIterable.iterator.takeWhile(self.upper.contains(_))
    }
  }
  /** Goes from highest to lowest for all items
   * that are contained in this Intersection
   */
  def greatestToLeast(implicit p: Predecessible[T]): Iterable[T] = {
    val self = this
    new AbstractIterable[T] {
      // we have to do this because the normal takeWhile causes OOM on big intervals
      def iterator = upper.toIterable.iterator.takeWhile(self.lower.contains(_))
    }
  }
}
