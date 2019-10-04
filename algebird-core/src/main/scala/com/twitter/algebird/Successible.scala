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
trait Successible[T] extends Serializable {
  def next(old: T): Option[T]
  def next(old: Option[T]): Option[T] = old.flatMap(next)
  def iterateNext(old: T): Iterable[T] = {
    val self = this
    // TODO in scala 2.11, there is an AbstractIterable which should be used here
    // to reduce generated class size due to all the methods in Iterable.
    // https://github.com/twitter/algebird/issues/263
    new AbstractIterable[T] {
      override def iterator: Iterator[T] =
        Iterator
          .iterate[Option[T]](Some(old)) { self.next(_) }
          .takeWhile(_.isDefined)
          .collect { case Some(t) => t }
    }
  }

  /**
   * The law is:
   * {{{
   * next(t).map(ordering.lt(t, _)).getOrElse(true)
   * }}}
   */
  def ordering: Ordering[T]

  @deprecated("use ordering instead", since = "0.13.0")
  final def partialOrdering: PartialOrdering[T] = ordering
}

object Successible {

  /**
   * This makes it easy to construct from a function when T has an ordering, which is common
   * Note, your function must respect the ordering
   */
  def fromNextOrd[T](nextFn: T => Option[T])(implicit ord: Ordering[T]): Successible[T] = new Successible[T] {
    override def next(t: T): Option[T] = nextFn(t)
    override def ordering: Ordering[T] = ord
  }
  // enables: Successible.next(2) == Some(3)
  def next[T](t: T)(implicit succ: Successible[T]): Option[T] = succ.next(t)
  def next[T](t: Option[T])(implicit succ: Successible[T]): Option[T] =
    succ.next(t)
  def iterateNext[T](old: T)(implicit succ: Successible[T]): Iterable[T] =
    succ.iterateNext(old)

  implicit def integralSucc[N: Integral]: Successible[N] =
    new IntegralSuccessible[N]

  /**
   * The difference between this and the default ordering on Option[T] is that it treats None
   * as the max value, instead of the minimum value.
   */
  def optionOrdering[T](implicit ord: Ordering[T]): Ordering[Option[T]] =
    new Ordering[Option[T]] {
      override def compare(left: Option[T], right: Option[T]): Int =
        (left, right) match {
          case (Some(l), Some(r)) => ord.compare(l, r)
          case (Some(_), None)    => -1
          case (None, Some(_))    => 1
          case (None, None)       => 0
        }
    }
}

object IntegralSuccessible {
  def next[T](old: T)(implicit integral: Integral[T]): Option[T] = {
    val newV = integral.plus(old, integral.one)
    if (integral.compare(newV, old) <= 0) {
      None
    } else {
      Some(newV)
    }
  }
}

class IntegralSuccessible[T: Integral] extends Successible[T] {
  private[this] val integral = implicitly[Integral[T]]

  override def next(old: T): Option[T] = IntegralSuccessible.next(old)

  override def ordering: Integral[T] = integral
}
