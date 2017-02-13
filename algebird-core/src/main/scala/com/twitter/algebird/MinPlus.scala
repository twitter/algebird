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

import algebra.ring.Rig

/*
 * The Min-Plus algebra, or tropical semi-ring is useful for computing shortest
 * paths on graphs:
 * @see <a href="http://en.wikipedia.org/wiki/Min-plus_matrix_multiplication">Min-plus Matrix Product"</a>
 * The shortest path from i->j in k or less steps is ((G)^k)_{ij}
 * @see <a href="http://en.wikipedia.org/wiki/Tropical_geometry">Tropical Geometry</a>
 * @see <a href="http://en.wikipedia.org/wiki/Semiring">Semiring definition</a>
 */

// This is basically a sigil class to represent using the Min-Plus semi-ring
sealed trait MinPlus[+V] extends Any with java.io.Serializable
case object MinPlusZero extends MinPlus[Nothing]
case class MinPlusValue[V](get: V) extends AnyVal with MinPlus[V]

class MinPlusSemiring[V](implicit monoid: Monoid[V], ord: Ordering[V]) extends Rig[MinPlus[V]] {
  override def zero = MinPlusZero
  override def one: MinPlus[V] = MinPlusValue(monoid.zero)
  // a+b = min(a,b)
  override def plus(left: MinPlus[V], right: MinPlus[V]) =
    // We are doing the if to avoid an allocation:
    (left, right) match {
      case (MinPlusZero, _) => right
      case (_, MinPlusZero) => left
      case (MinPlusValue(lv), MinPlusValue(rv)) => if (ord.lteq(lv, rv)) left else right
    }

  // a*b = a+b
  override def times(left: MinPlus[V], right: MinPlus[V]) =
    (left, right) match {
      case (MinPlusZero, _) => MinPlusZero
      case (_, MinPlusZero) => MinPlusZero
      case (MinPlusValue(lv), MinPlusValue(rv)) => MinPlusValue(monoid.plus(lv, rv))
    }
}

object MinPlus extends java.io.Serializable {
  /**
   * This is unsafe but here for legacy reasons
   */
  implicit def semiring[V: Monoid: Ordering]: Ring[MinPlus[V]] = new UnsafeFromAlgebraRig(rig[V])
  implicit def rig[V: Monoid: Ordering]: Rig[MinPlus[V]] = new MinPlusSemiring[V]
}
