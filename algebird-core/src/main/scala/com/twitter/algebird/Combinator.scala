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

import scala.annotation.tailrec
import scala.annotation.implicitNotFound
import scala.math.Equiv

/** This is a combinator on semigroups, after you do the plus, you transform B with a fold function
 * This will not be valid for all fold functions. You need to prove that it is still associative.
 *
 * Clearly only values of (a,b) are valid if fold(a,b) == b, so keep that in mind.
 *
 * I have not yet found a sufficient condition on (A,B) => B that makes it correct
 * Clearly a (trivial) constant function {(l,r) => r} works.
 * Also, if B is List[T], and (l:A,r:List[T]) = r.sortBy(fn(l))
 * this works as well (due to the associativity on A, and the fact that the list never loses data).
 *
 * For approximate lists (like top-K applications) this might work (or be close enough to associative
 * that for approximation algorithms it is fine), and in fact, that is the main motivation of this code:
 * Produce some ordering in A, and use it to do sorted-topK on the list in B.
 *
 * Seems like an open topic here.... you are obliged to think on your own about this.
 */
class SemigroupCombinator[A:Semigroup,B:Semigroup](fold: (A,B) => B) extends Semigroup[(A,B)] {
  def plus(left: (A,B), right: (A,B)) = {
    val first = Semigroup.plus(left._1, right._1)
    (first, fold(first, Semigroup.plus(left._2, right._2)))
  }
}

class MonoidCombinator[A:Monoid,B:Monoid](fold: (A,B) => B) extends SemigroupCombinator[A,B](fold) with Monoid[(A,B)] {
  def zero = (Monoid.zero[A], Monoid.zero[B])
}
