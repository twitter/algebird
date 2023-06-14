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

/**
 * Represents a single interval on a T with an Ordering
 */
sealed trait Interval[T] extends java.io.Serializable {
  def contains(t: T)(implicit ord: Ordering[T]): Boolean

  def intersect(that: Interval[T])(implicit ord: Ordering[T]): Interval[T]
  final def apply(t: T)(implicit ord: Ordering[T]): Boolean = contains(t)
  final def &&(that: Interval[T])(implicit ord: Ordering[T]): Interval[T] = intersect(that)

  /**
   * Map the Interval with a non-decreasing function. If you use a non-monotonic function (like x^2) then the
   * result is meaningless. TODO: It might be good to have types for these properties in algebird.
   */
  def mapNonDecreasing[U](fn: T => U): Interval[U]
}

case class Universe[T]() extends Interval[T] {
  override def contains(t: T)(implicit ord: Ordering[T]): Boolean = true
  override def intersect(that: Interval[T])(implicit ord: Ordering[T]): Interval[T] =
    that
  override def mapNonDecreasing[U](fn: T => U): Interval[U] = Universe()
}

case class Empty[T]() extends Interval[T] {
  override def contains(t: T)(implicit ord: Ordering[T]): Boolean = false
  override def intersect(that: Interval[T])(implicit ord: Ordering[T]): Interval[T] =
    this
  override def mapNonDecreasing[U](fn: T => U): Interval[U] = Empty()
}

object Interval extends java.io.Serializable {

  /**
   * Class that only exists so that [[leftClosedRightOpen]] and [[leftOpenRightClosed]] can retain the type
   * information of the returned interval. The compiler doesn't know anything about ordering, so without
   * [[MaybeEmpty]] the only valid return type is Interval[T].
   */
  sealed abstract class MaybeEmpty[T, NonEmpty[t] <: Interval[t]] {
    def isEmpty: Boolean
  }
  object MaybeEmpty {

    /**
     * Represents an empty interval.
     */
    case class SoEmpty[T, NonEmpty[t] <: Interval[t]]() extends MaybeEmpty[T, NonEmpty] {
      override def isEmpty: Boolean = true
    }

    /**
     * Represents a non-empty interval.
     */
    case class NotSoEmpty[T, NonEmpty[t] <: Interval[t]](get: NonEmpty[T]) extends MaybeEmpty[T, NonEmpty] {
      override def isEmpty: Boolean = false
    }
  }

  type GenIntersection[T] = Intersection[Lower, Upper, T]
  type InLowExUp[T] = Intersection[InclusiveLower, ExclusiveUpper, T]
  type InLowInUp[T] = Intersection[InclusiveLower, InclusiveUpper, T]
  type ExLowExUp[T] = Intersection[ExclusiveLower, ExclusiveUpper, T]
  type ExLowInUp[T] = Intersection[ExclusiveLower, InclusiveUpper, T]

  implicit def monoid[T: Ordering]: Monoid[Interval[T]] =
    Monoid.from[Interval[T]](Universe[T]())(_ && _)

  // Automatically convert from a MaybeEmpty instance
  implicit def fromMaybeEmpty[T, NonEmpty[t] <: Interval[t]](me: MaybeEmpty[T, NonEmpty]): Interval[T] =
    me match {
      case MaybeEmpty.SoEmpty()     => Empty()
      case MaybeEmpty.NotSoEmpty(i) => i
    }

  def leftClosedRightOpen[T: Ordering](lower: T, upper: T): MaybeEmpty[T, InLowExUp] =
    if (Ordering[T].lt(lower, upper))
      MaybeEmpty.NotSoEmpty[T, InLowExUp](Intersection(InclusiveLower(lower), ExclusiveUpper(upper)))
    else MaybeEmpty.SoEmpty[T, InLowExUp]()

  def leftOpenRightClosed[T: Ordering](lower: T, upper: T): MaybeEmpty[T, ExLowInUp] =
    if (Ordering[T].lt(lower, upper))
      MaybeEmpty.NotSoEmpty[T, ExLowInUp](Intersection(ExclusiveLower(lower), InclusiveUpper(upper)))
    else MaybeEmpty.SoEmpty[T, ExLowInUp]()

  def closed[T: Ordering](lower: T, upper: T): MaybeEmpty[T, InLowInUp] =
    if (Ordering[T].lteq(lower, upper))
      MaybeEmpty.NotSoEmpty[T, InLowInUp](Intersection(InclusiveLower(lower), InclusiveUpper(upper)))
    else MaybeEmpty.SoEmpty[T, InLowInUp]()

  def open[T: Ordering](lower: T, upper: T): MaybeEmpty[T, ExLowExUp] =
    if (Ordering[T].lt(lower, upper))
      MaybeEmpty.NotSoEmpty[T, ExLowExUp](Intersection(ExclusiveLower(lower), ExclusiveUpper(upper)))
    else MaybeEmpty.SoEmpty[T, ExLowExUp]()

  /**
   * This is here for binary compatibility reasons. These methods should be moved to Interval, which should
   * also be an abstract class for better binary compatibility at the next incompatible change
   */
  implicit final class IntervalMethods[T](val intr: Interval[T]) extends AnyVal {
    def isEmpty(implicit succ: Successible[T], pred: Predecessible[T]): Boolean = intr match {
      case Empty()    => true
      case Universe() => false
      case Intersection(InclusiveLower(l), ExclusiveUpper(u)) =>
        !succ.ordering.lt(l, u)
      case Intersection(InclusiveLower(l), InclusiveUpper(u)) =>
        !succ.ordering.lteq(l, u)
      case Intersection(ExclusiveLower(l), ExclusiveUpper(u)) =>
        !succ.next(l).exists(succ.ordering.lt(_, u))
      case Intersection(ExclusiveLower(l), InclusiveUpper(u)) =>
        !succ.next(l).exists(succ.ordering.lteq(_, u))
      case InclusiveLower(_) => false // we at least have l
      case InclusiveUpper(_) => false // false // we at least have u
      case ExclusiveLower(l) =>
        succ.next(l).isEmpty
      case ExclusiveUpper(u) =>
        pred.prev(u).isEmpty
    }

    /**
     * If this returns Some(t), then intr.contains(t) and there is no s less than t such that intr.contains(s)
     *
     * if this returns None, it may be Empty, Upper or Universe
     */
    def boundedLeast(implicit succ: Successible[T]): Option[T] = intr match {
      case Empty()                => None
      case Universe()             => None
      case _: Upper[_]            => None
      case i @ Intersection(_, _) => i.least
      case l: Lower[_]            => l.least
    }

    /**
     * If this returns Some(t), then intr.contains(t) and there is no s greater than t such that
     * intr.contains(s)
     *
     * if this returns None, it may be Empty, Lower, or Universe
     */
    def boundedGreatest(implicit pred: Predecessible[T]): Option[T] =
      intr match {
        case Empty()                => None
        case Universe()             => None
        case _: Lower[_]            => None
        case i @ Intersection(_, _) => i.greatest
        case u: Upper[_]            => u.greatest
      }
  }
}

// Marker traits to keep lower on the left in Intersection
sealed trait Lower[T] extends Interval[T] {

  /**
   * This may give a false positive (but should try not to). Note the case of (0,1) for the integers. If they
   * were doubles, this would intersect, but since there are no members of the set Int that are bigger than 0
   * and less than 1, they don't really intersect. So, ordering is not enough here. You need a stronger
   * notion, which we don't have a typeclass for.
   */
  def intersects(u: Upper[T])(implicit ord: Ordering[T]): Boolean

  /**
   * The smallest value that is contained here This is an Option, because of cases like
   * ExclusiveLower(Int.MaxValue) which are pathological and equivalent to Empty
   */
  def least(implicit s: Successible[T]): Option[T]
  def strictLowerBound(implicit p: Predecessible[T]): Option[T]

  /**
   * Iterates all the items in this Lower[T] from lowest to highest
   */
  def toIterable(implicit s: Successible[T]): Iterable[T] =
    least match {
      case Some(l) => s.iterateNext(l)
      case None    => Iterable.empty
    }
}
sealed trait Upper[T] extends Interval[T] {

  /**
   * The smallest value that is contained here This is an Option, because of cases like
   * ExclusiveUpper(Int.MinValue), which are pathological and equivalent to Empty
   */
  def greatest(implicit p: Predecessible[T]): Option[T]
  // The smallest value that is not present
  def strictUpperBound(implicit s: Successible[T]): Option[T]

  /**
   * Iterates all the items in this Upper[T] from highest to lowest
   */
  def toIterable(implicit p: Predecessible[T]): Iterable[T] =
    greatest match {
      case Some(g) => p.iteratePrev(g)
      case None    => Iterable.empty
    }
}

case class InclusiveLower[T](lower: T) extends Interval[T] with Lower[T] {
  override def contains(t: T)(implicit ordering: Ordering[T]): Boolean =
    ordering.lteq(lower, t)
  override def intersect(that: Interval[T])(implicit ordering: Ordering[T]): Interval[T] = that match {
    case Universe() => this
    case Empty()    => that
    case ub @ InclusiveUpper(_) =>
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case ub @ ExclusiveUpper(_) =>
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case InclusiveLower(thatlb) =>
      if (ordering.gt(lower, thatlb)) this else that
    case ExclusiveLower(thatlb) =>
      if (ordering.gt(lower, thatlb)) this else that
    case Intersection(thatL, thatU) => (this && thatL) && thatU
  }
  override def intersects(u: Upper[T])(implicit ordering: Ordering[T]): Boolean =
    u match {
      case InclusiveUpper(upper) => ordering.lteq(lower, upper)
      case ExclusiveUpper(upper) => ordering.lt(lower, upper)
    }
  override def least(implicit s: Successible[T]): Option[T] = Some(lower)
  override def strictLowerBound(implicit p: Predecessible[T]): Option[T] = p.prev(lower)
  override def mapNonDecreasing[U](fn: T => U): Interval[U] = InclusiveLower(fn(lower))
}
case class ExclusiveLower[T](lower: T) extends Interval[T] with Lower[T] {
  override def contains(t: T)(implicit ordering: Ordering[T]): Boolean =
    ordering.lt(lower, t)
  override def intersect(that: Interval[T])(implicit ordering: Ordering[T]): Interval[T] = that match {
    case Universe() => this
    case Empty()    => that
    case ub @ InclusiveUpper(_) =>
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case ub @ ExclusiveUpper(_) =>
      if (intersects(ub)) Intersection(this, ub) else Empty()
    case InclusiveLower(thatlb) =>
      if (ordering.gteq(lower, thatlb)) this else that
    case ExclusiveLower(thatlb) =>
      if (ordering.gteq(lower, thatlb)) this else that
    case Intersection(thatL, thatU) => (this && thatL) && thatU
  }
  override def intersects(u: Upper[T])(implicit ordering: Ordering[T]): Boolean =
    u match {
      case InclusiveUpper(upper) => ordering.lt(lower, upper)
      case ExclusiveUpper(upper) =>
        ordering.lt(lower, upper) // This is a false positive for (x, next(x))
    }
  override def least(implicit s: Successible[T]): Option[T] = s.next(lower)
  override def strictLowerBound(implicit p: Predecessible[T]): Option[T] = Some(lower)
  override def mapNonDecreasing[U](fn: T => U): Interval[U] = ExclusiveLower(fn(lower))
}
case class InclusiveUpper[T](upper: T) extends Interval[T] with Upper[T] {
  override def contains(t: T)(implicit ordering: Ordering[T]): Boolean =
    ordering.lteq(t, upper)
  override def greatest(implicit p: Predecessible[T]): Option[T] = Some(upper)
  // The smallest value that is not present
  override def strictUpperBound(implicit s: Successible[T]): Option[T] = s.next(upper)
  override def intersect(that: Interval[T])(implicit ordering: Ordering[T]): Interval[T] = that match {
    case Universe() => this
    case Empty()    => that
    case lb @ InclusiveLower(_) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
    case lb @ ExclusiveLower(_) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
    case InclusiveUpper(thatub) =>
      if (ordering.lt(upper, thatub)) this else that
    case ExclusiveUpper(thatub) =>
      if (ordering.lt(upper, thatub)) this else that
    case Intersection(thatL, thatU) => thatL && (this && thatU)
  }
  override def mapNonDecreasing[U](fn: T => U): Interval[U] = InclusiveUpper(fn(upper))
}
case class ExclusiveUpper[T](upper: T) extends Interval[T] with Upper[T] {
  override def contains(t: T)(implicit ordering: Ordering[T]): Boolean =
    ordering.lt(t, upper)
  override def greatest(implicit p: Predecessible[T]): Option[T] = p.prev(upper)
  // The smallest value that is not present
  override def strictUpperBound(implicit s: Successible[T]): Option[T] = Some(upper)
  override def intersect(that: Interval[T])(implicit ordering: Ordering[T]): Interval[T] = that match {
    case Universe() => this
    case Empty()    => that
    case lb @ InclusiveLower(_) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
    case lb @ ExclusiveLower(_) =>
      if (lb.intersects(this)) Intersection(lb, this) else Empty()
    case InclusiveUpper(thatub) =>
      if (ordering.lteq(upper, thatub)) this else that
    case ExclusiveUpper(thatub) =>
      if (ordering.lteq(upper, thatub)) this else that
    case Intersection(thatL, thatU) => thatL && (this && thatU)
  }
  override def mapNonDecreasing[U](fn: T => U): Interval[U] = ExclusiveUpper(fn(upper))
}

case class Intersection[L[t] <: Lower[t], U[t] <: Upper[t], T](lower: L[T], upper: U[T]) extends Interval[T] {
  override def contains(t: T)(implicit ordering: Ordering[T]): Boolean =
    lower.contains(t) && upper.contains(t)
  override def intersect(that: Interval[T])(implicit ordering: Ordering[T]): Interval[T] = that match {
    case Universe()                 => this
    case Empty()                    => that
    case lb @ InclusiveLower(_)     => (lb && lower) && upper
    case lb @ ExclusiveLower(_)     => (lb && lower) && upper
    case ub @ InclusiveUpper(_)     => lower && (ub && upper)
    case ub @ ExclusiveUpper(_)     => lower && (ub && upper)
    case Intersection(thatL, thatU) => (lower && thatL) && (upper && thatU)
  }
  override def mapNonDecreasing[T1](fn: T => T1): Interval[T1] = {
    val newLower = lower match {
      case InclusiveLower(l) => InclusiveLower(fn(l))
      case ExclusiveLower(l) => ExclusiveLower(fn(l))
    }
    val newUpper = upper match {
      case InclusiveUpper(u) => InclusiveUpper(fn(u))
      case ExclusiveUpper(u) => ExclusiveUpper(fn(u))
    }
    Intersection(newLower, newUpper)
  }

  def least(implicit s: Successible[T]): Option[T] =
    lower.least.filter(upper.contains(_)(s.ordering))

  /**
   * Goes from lowest to highest for all items that are contained in this Intersection
   */
  def leastToGreatest(implicit s: Successible[T]): Iterable[T] = {
    val self = this
    implicit val ord: Ordering[T] = s.ordering
    // TODO https://github.com/twitter/algebird/issues/263
    new AbstractIterable[T] {
      // We have to do this because the normal takeWhile causes OOM on big intervals
      override def iterator: Iterator[T] = lower.toIterable.iterator.takeWhile(self.upper.contains(_))
    }
  }

  def greatest(implicit p: Predecessible[T]): Option[T] =
    upper.greatest.filter(lower.contains(_)(p.ordering))

  /**
   * Goes from highest to lowest for all items that are contained in this Intersection
   */
  def greatestToLeast(implicit p: Predecessible[T]): Iterable[T] = {
    val self = this
    implicit val ord: Ordering[T] = p.ordering
    // TODO https://github.com/twitter/algebird/issues/263
    new AbstractIterable[T] {
      // We have to do this because the normal takeWhile causes OOM on big intervals
      override def iterator: Iterator[T] = upper.toIterable.iterator.takeWhile(self.lower.contains(_))
    }
  }

  /**
   * Some intervals can actually be synonyms for empty: (0,0) for instance, contains nothing. This cannot be
   * normalized to [a, b) form, thus we return an option Also, there are cases like [Int.MinValue,
   * Int.MaxValue] that cannot are actually equivalent to Universe. The bottom line: if this returns None, it
   * just means you can't express it this way, it does not mean it is empty or universe, etc... (there are
   * other cases).
   */
  def toLeftClosedRightOpen(implicit
      s: Successible[T]
  ): Option[Intersection[InclusiveLower, ExclusiveUpper, T]] =
    for {
      l <- lower.least
      g <- upper.strictUpperBound if s.ordering.lt(l, g)
    } yield Intersection(InclusiveLower(l), ExclusiveUpper(g))
}
