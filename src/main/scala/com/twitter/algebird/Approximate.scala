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

trait Approximate[T] {
   implicit def numeric: Numeric[T]
   // we can assume there is an implicit numeric on T below:
   def estimate : T
   def max : T
   def min : T
   def probWithinBounds : Double
   // is this value contained within the bounds:
   def boundsContain(v: T) = numeric.lteq(min, v) && numeric.lteq(v, max)
   /*
    * Contract is:
    * Prob(boundsContain(estimate)) >= probWithinBounds
    */
   def isExact: Boolean = numeric.equiv(min, max)
   def +(right: Approximate[T]): Approximate[T] = {
     val n = numeric
     Approximate(n.plus(min, right.min),
       n.plus(estimate, right.estimate),
       n.plus(max, right.max),
       probWithinBounds * right.probWithinBounds)
   }
   override lazy val toString =
     "Approximate" + (min, estimate, max, probWithinBounds).toString
   override def equals(that: Any): Boolean =
     that.isInstanceOf[Approximate[T]] && {
       val thatA = that.asInstanceOf[Approximate[T]]
       (min, estimate, max, probWithinBounds) ==
         (thatA.min, thatA.estimate, thatA.max, thatA.probWithinBounds)
     }
}

object Approximate {
  // Probably most common constructor:
  def apply[T](mn: T, est: T, mx: T, prob: Double)(implicit n: Numeric[T]) =
    new Approximate[T] {
      val numeric = n
      val estimate = est
      val max = mx
      val min = mn
      val probWithinBounds = prob
    }
  def exact[T](v: T)(implicit n: Numeric[T]) =
    new Approximate[T] {
      val numeric = n
      val estimate = v
      val max = v
      val min = v
      val probWithinBounds = 1.0
    }
  def zero[N](implicit n: Numeric[N]) = Approximate.exact(n.zero)

  implicit def monoid[N](implicit n: Numeric[N]): Monoid[Approximate[N]] =
    new Monoid[Approximate[N]] {
      lazy val zero = Approximate.zero[N]
      def plus(left: Approximate[N], right: Approximate[N]) =
        left + right
    }
}
