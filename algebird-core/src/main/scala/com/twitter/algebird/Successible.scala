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
 * This is a monoid to represent things which increase. Note that it is important
 * that a value after being incremented is always larger than it was before. Note
 * that next returns Option because this class comes with the notion of the "greatest"
 * key, which is None. Ints, for example, will cycle if next(java.lang.Integer.MAX_VALUE)
 * is called, therefore we need a notion of what happens when we hit the bounds at
 * which our ordering is violating. This is also useful for closed sets which have a fixed
 * progression.
 */
trait Successible[@specialized(Int,Long,Float,Double) T] {
  def next(old: T): Option[T]
  def next(old: Option[T]): Option[T] = old flatMap next
  def ordering: Ordering[T]
}

object Successible {
  implicit val intSuccessible = new NumericSuccessible[Int]
  implicit val longSuccessible = new NumericSuccessible[Long]

  /**
   * The difference between this and the default ordering on Option[T] is that it treats None
   * as the max value, instead of the minimum value.
   */
  def optionOrdering[@specialized(Int,Long,Float,Double) T](implicit ord: Ordering[T]): Ordering[Option[T]] = new Ordering[Option[T]] {
    def compare(left: Option[T], right: Option[T]) =
      (left, right) match {
        case (Some(l), Some(r)) => ord.compare(l, r)
        case (Some(l), None) => -1
        case (None, Some(r)) => 1
        case (None, None) => 0
      }
  }
}

class NumericSuccessible[@specialized(Int,Long,Float,Double) T:Numeric:Ordering] extends Successible[T] {
  def next(old: T) = {
    val numeric = implicitly[Numeric[T]]
    val newV = numeric.plus(old, numeric.one)
    if (ordering.compare(newV, old) <= 0) {
      None
    } else {
      Some(newV)
    }
  }

  def ordering = implicitly[Ordering[T]]
}
