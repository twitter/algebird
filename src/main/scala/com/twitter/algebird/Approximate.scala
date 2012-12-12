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

// Note the probWithinBounds is a LOWER BOUND (at least this probability)
case class Approximate[N](min: N, estimate: N, max: N, probWithinBounds: Double)
  (implicit val numeric: Numeric[N]) {
   // is this value contained within the bounds:
   def boundsContain(v: N): Boolean = numeric.lteq(min, v) && numeric.lteq(v, max)
   /*
    * Nhis is so you can do: val x = Approximate(1.0, 1.1, 1.2, 0.99)
    * and then x ~ 1.05 returns true
    */
   def ~(v:N): Boolean = boundsContain(v)
   /*
    * Contract is:
    * Prob(boundsContain(estimate)) >= probWithinBounds
    */
   def isExact: Boolean = (probWithinBounds == 1.0) && numeric.equiv(min, max)
   def +(right: Approximate[N]): Approximate[N] = {
     val n = numeric
     Approximate(n.plus(min, right.min),
       n.plus(estimate, right.estimate),
       n.plus(max, right.max),
       probWithinBounds * right.probWithinBounds)
   }
   /** This is not distributive, because:
    * a*(b+c) has two probability multiplications
    * while (a*b + a*b) has three
    * Some kind of general formula solver could possibly
    * make this distributive, but in the mean time, it's only
    * a group
    */
   def *(right: Approximate[N]): Approximate[N] =
     if(right.isZero || isOne) {
       right
     }
     else if(isZero || right.isOne) {
       this
     }
     else {
       val n = numeric
       val ends = for(leftv <- List(min, max);
                      rightv <- List(right.min, right.max))
                  yield n.times(leftv, rightv)

       val newProb = probWithinBounds * right.probWithinBounds

       Approximate(ends.min, n.times(estimate, right.estimate),
         ends.max, newProb)
     }

   def isZero: Boolean = {
     isExact && numeric.equiv(estimate, numeric.zero)
   }
   def isOne: Boolean = {
     isExact && numeric.equiv(estimate, numeric.one)
   }

   def negate = {
    this * Approximate.exact(numeric.negate(numeric.one))
  }
}

object Approximate {
  def exact[N:Numeric](v: N) = Approximate(v,v,v,1.0)
  def zero[N](implicit n: Numeric[N]) = exact(n.zero)
  def one[N](implicit n: Numeric[N]) = exact(n.one)
  // Not a group/ring:
  // negate fails: x - x != 0, because with some probability the bound is bad.
  // distributive fails because a*b + a*c ignores that a is either in or out
  // of the bound, and counts it idependently.
  implicit def monoid[N](implicit n: Numeric[N]): Monoid[Approximate[N]] = {
    // avoid capturing the Numeric:
    val z = Approximate.zero[N]
    new Monoid[Approximate[N]] {
      val zero = z
      override def isNonZero(v: Approximate[N]) = !v.isZero
      def plus(left: Approximate[N], right: Approximate[N]) = left + right
    }
  }
}
