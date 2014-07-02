/*
Copyright 2014 Twitter, Inc.

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
 * This is a typeclass to represent things which are countable down. Note that it is important
 * that a value prev(t) is always less than t. Note
 * that prev returns Option because this class comes with the notion that some items may reach a minimum
 * key, which is None.
 */
trait Predecessible[T] extends java.io.Serializable {
  def prev(old: T): Option[T]
  def prev(old: Option[T]): Option[T] = old.flatMap(prev)
  def iteratePrev(old: T): Iterable[T] = {
    val self = this
    // TODO in scala 2.11, there is an AbstractIterable which should be used here
    // to reduce generated class size due to all the methods in Iterable.
    // https://github.com/twitter/algebird/issues/263
    new AbstractIterable[T] {
      def iterator =
        Iterator.iterate[Option[T]](Some(old)) { self.prev(_) }
          .takeWhile(_.isDefined)
          .map(_.get)
    }
  }
  def ordering: Ordering[T]
}

object Predecessible extends java.io.Serializable {
  // enables: Predecessible.prev(2) == Some(1)
  def prev[T](t: T)(implicit p: Predecessible[T]): Option[T] = p.prev(t)
  def prev[T](t: Option[T])(implicit p: Predecessible[T]): Option[T] = p.prev(t)

  def iteratePrev[T](first: T)(implicit p: Predecessible[T]): Iterable[T] =
    p.iteratePrev(first)

  implicit def integralPrev[N: Integral]: Predecessible[N] = new IntegralPredecessible[N]
}

class IntegralPredecessible[T: Integral] extends Predecessible[T] {
  def prev(old: T) = {
    val numeric = implicitly[Integral[T]]
    val newV = numeric.minus(old, numeric.one)
    if (ordering.compare(newV, old) >= 0) {
      // We wrapped around
      None
    } else {
      Some(newV)
    }
  }

  def ordering: Ordering[T] = implicitly[Integral[T]]
}
