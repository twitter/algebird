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

/**
 * This is a typeclass to represent things which increase. Note that it is important
 * that a value after being incremented is always larger than it was before. Note
 * that next returns Option because this class comes with the notion of the "greatest"
 * key, which is None. Ints, for example, will cycle if next(java.lang.Integer.MAX_VALUE)
 * is called, therefore we need a notion of what happens when we hit the bounds at
 * which our ordering is violating. This is also useful for closed sets which have a fixed
 * progression.
 */
trait Successible[T] {
  def next(old: T): Option[T]
  def next(old: Option[T]): Option[T] = old.flatMap(next)
  def iterateNext(old: T): Iterable[T] = {
    val self = this
    // TODO in scala 2.11, there is an AbstractIterable which should be used here
    // to reduce generated class size due to all the methods in Iterable.
    // https://github.com/twitter/algebird/issues/263
    new AbstractIterable[T] {
      def iterator =
        Iterator.iterate[Option[T]](Some(old)) { self.next(_) }
          .takeWhile(_.isDefined)
          .collect { case Some(t) => t }
    }
  }
  /**
   * The law is:
   * next(t)
   *   .map { n => partialOrdering.lteq(t, n) && (!partialOrdering.equiv(t, n)) }
   *   .getOrElse(true)
   *
   * Note Ordering extends PartialOrdering, so we are taking a weak constraint
   * that some items can be ordered, and namely, the sequence of items returned
   * by next is strictly increasing
   */
  def partialOrdering: PartialOrdering[T]
}

object Successible {
  /**
   * This makes it easy to construct from a function when T has an ordering, which is common
   * Note, your function must respect the ordering
   */
  def fromNextOrd[T](nextFn: T => Option[T])(implicit ord: Ordering[T]): Successible[T] = new Successible[T] {
    def next(t: T) = nextFn(t)
    def partialOrdering = ord
  }
  // enables: Successible.next(2) == Some(3)
  def next[T](t: T)(implicit succ: Successible[T]): Option[T] = succ.next(t)
  def next[T](t: Option[T])(implicit succ: Successible[T]): Option[T] = succ.next(t)
  def iterateNext[T](old: T)(implicit succ: Successible[T]): Iterable[T] =
    succ.iterateNext(old)

  implicit def integralSucc[N: Integral]: Successible[N] = new IntegralSuccessible[N]

  /**
   * The difference between this and the default ordering on Option[T] is that it treats None
   * as the max value, instead of the minimum value.
   */
  def optionOrdering[T](implicit ord: Ordering[T]): Ordering[Option[T]] = new Ordering[Option[T]] {
    def compare(left: Option[T], right: Option[T]) =
      (left, right) match {
        case (Some(l), Some(r)) => ord.compare(l, r)
        case (Some(l), None) => -1
        case (None, Some(r)) => 1
        case (None, None) => 0
      }
  }
  // For some sad reason, scala does not define implicit PartialOrderings this way
  implicit def partialFromOrdering[T](implicit ord: Ordering[T]): PartialOrdering[T] = ord
}

class IntegralSuccessible[T: Integral] extends Successible[T] {
  private[this] val integral = implicitly[Integral[T]]
  def next(old: T) = {
    val newV = integral.plus(old, integral.one)
    if (integral.compare(newV, old) <= 0) {
      None
    } else {
      Some(newV)
    }
  }

  def partialOrdering = integral
}
