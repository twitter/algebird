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
 */
sealed trait Interval[T] extends java.io.Serializable {
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
  type GenIntersection[T] = Intersection[Lower, Upper, T]
  type InLowExUp[T] = Intersection[InclusiveLower, ExclusiveUpper, T]
  type InLowInUp[T] = Intersection[InclusiveLower, InclusiveUpper, T]
  type ExLowExUp[T] = Intersection[ExclusiveLower, ExclusiveUpper, T]
  type ExLowInUp[T] = Intersection[ExclusiveLower, InclusiveUpper, T]

  implicit def monoid[T]: Monoid[Interval[T]] = Monoid.from[Interval[T]](Universe[T]()) { _ && _ }

  // Automatically convert from an either
  implicit def fromEither[L[t] <: Interval[t], R[t] <: Interval[t], T](either: Either[L[T],R[T]]): Interval[T] =
    either match {
      case Right(i) => i
      case Left(i) => i
    }

  def leftClosedRightOpen[T:Ordering](lower: T, upper: T): Either[Empty[T], InLowExUp[T]] =
    if(Ordering[T].lt(lower, upper))
      Right(Intersection(InclusiveLower(lower), ExclusiveUpper(upper)))
    else Left(Empty())

  def leftOpenRightClosed[T:Ordering](lower: T, upper: T): Either[Empty[T], ExLowInUp[T]] =
    if(Ordering[T].lt(lower, upper))
      Right(Intersection(ExclusiveLower(lower), InclusiveUpper(upper)))
    else Left(Empty())
}

// Marker traits to keep lower on the left in Intersection
sealed trait Lower[T] extends Interval[T] {
  def ordering: Ordering[T]
  /**
   * This may give a false positive (but should try not to).
   * Note the case of (0,1) for the integers. If they were doubles,
   * this would intersect, but since there are no members of the
   * set Int that are bigger than 0 and less than 1, they don't really
   * intersect. So, ordering is not enough here. You need a stronger
   * notion, which we don't have a typeclass for.
   */
  def intersects(u: Upper[T]): Boolean
  /**
   * The smallest value that is contained here
   * This is an Option, because of cases like ExclusiveLower(Int.MaxValue)
   * which are pathological and equivalent to Empty
   */
  def least(implicit s: Successible[T]): Option[T]
  def strictLowerBound(implicit p: Predecessible[T]): Option[T]
  /** Iterates all the items in this Lower[T] from lowest to highest
   */
  def toIterable(implicit s: Successible[T]): Iterable[T] =
    least match {
      case Some(l) => s.iterateNext(l)
      case None => Iterable.empty
    }
}
sealed trait Upper[T] extends Interval[T] {
  def ordering: Ordering[T]
  /**
   * The smallest value that is contained here
   * This is an Option, because of cases like ExclusiveUpper(Int.MinValue),
   * which are pathological and equivalent to Empty
   */
  def greatest(implicit p: Predecessible[T]): Option[T]
  // The smallest value that is not present
  def strictUpperBound(implicit s: Successible[T]): Option[T]
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
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case ub@ExclusiveUpper(upper) =>
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case lb@InclusiveLower(thatlb) => if (lb.ordering.gt(lower, thatlb)) this else that
    case lb@ExclusiveLower(thatlb) => if (lb.ordering.gt(lower, thatlb)) this else that
    case Intersection(thatL, thatU) => (this && thatL) && thatU
  }
  def intersects(u: Upper[T]): Boolean = u match {
    case InclusiveUpper(upper) => ordering.lteq(lower, upper)
    case ExclusiveUpper(upper) => ordering.lt(lower, upper)
  }
  def least(implicit s: Successible[T]): Option[T] = Some(lower)
  def strictLowerBound(implicit p: Predecessible[T]): Option[T] = p.prev(lower)
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = InclusiveLower(fn(lower))
}
case class ExclusiveLower[T](lower: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Lower[T] {
  def contains(t: T): Boolean = ordering.lt(lower, t)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Universe() => this
    case Empty() => that
    case ub@InclusiveUpper(upper) =>
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case ub@ExclusiveUpper(upper) =>
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case lb@InclusiveLower(thatlb) => if (lb.ordering.gteq(lower, thatlb)) this else that
    case lb@ExclusiveLower(thatlb) => if (lb.ordering.gteq(lower, thatlb)) this else that
    case Intersection(thatL, thatU) => (this && thatL) && thatU
  }
  def intersects(u: Upper[T]): Boolean = u match {
    case InclusiveUpper(upper) => ordering.lt(lower, upper)
    case ExclusiveUpper(upper) => ordering.lt(lower, upper) // This is a false positive for (x, next(x))
  }
  def least(implicit s: Successible[T]): Option[T] = s.next(lower)
  def strictLowerBound(implicit p: Predecessible[T]): Option[T] = Some(lower)
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = ExclusiveLower(fn(lower))
}
case class InclusiveUpper[T](upper: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Upper[T] {
  def contains(t: T): Boolean = ordering.lteq(t, upper)
  def greatest(implicit p: Predecessible[T]): Option[T] = Some(upper)
  // The smallest value that is not present
  def strictUpperBound(implicit s: Successible[T]): Option[T] = s.next(upper)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Universe() => this
    case Empty() => that
    case lb@InclusiveLower(lower) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
    case lb@ExclusiveLower(lower) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
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
  // The smallest value that is not present
  def strictUpperBound(implicit s: Successible[T]): Option[T] = Some(upper)
  def intersect(that: Interval[T]): Interval[T] = that match {
    case Universe() => this
    case Empty() => that
    case lb@InclusiveLower(lower) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
    case lb@ExclusiveLower(lower) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
    case ub@InclusiveUpper(thatub) =>
      if (ub.ordering.lteq(upper, thatub)) this else that
    case ub@ExclusiveUpper(thatub) =>
      if (ub.ordering.lteq(upper, thatub)) this else that
    case Intersection(thatL, thatU) => thatL && (this && thatU)
  }
  def mapNonDecreasing[U:Ordering](fn: T => U): Interval[U] = ExclusiveUpper(fn(upper))
}

case class Intersection[L[t] <: Lower[t], U[t] <: Upper[t], T](lower: L[T], upper: U[T]) extends Interval[T] {
  require(lower.intersects(upper), "Intersections must be non-empty: (%s, %s)".format(lower, upper))
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
    // TODO https://github.com/twitter/algebird/issues/263
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
    // TODO https://github.com/twitter/algebird/issues/263
    new AbstractIterable[T] {
      // we have to do this because the normal takeWhile causes OOM on big intervals
      def iterator = upper.toIterable.iterator.takeWhile(self.lower.contains(_))
    }
  }

  /**
   * Some intervals can actually be synonyms for empty:
   * (0,0) for instance, contains nothing. This cannot be normalized to
   * [a, b) form, thus we return an option
   * Also, there are cases like [Int.MinValue, Int.MaxValue] that cannot
   * are actually equivalent to Universe.
   * The bottom line: if this returns None, it just means you can't express
   * it this way, it does not mean it is empty or universe, etc... (there
   * are other cases).
   */
  def toLeftClosedRightOpen(implicit s: Successible[T]): Option[Intersection[InclusiveLower, ExclusiveUpper, T]] = {
    implicit val ord = lower.ordering
    for {
      l <- lower.least
      g <- upper.strictUpperBound if lower.ordering.lt(l, g)
    } yield Intersection(InclusiveLower(l), ExclusiveUpper(g))
  }
}
