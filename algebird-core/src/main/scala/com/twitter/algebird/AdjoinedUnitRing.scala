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

/**
 * This is for the case where your Ring[T] is a Rng (i.e. there is no unit).
 * @see http://en.wikipedia.org/wiki/Pseudo-ring#Adjoining_an_identity_element
 */
case class AdjoinedUnit[T](ones: BigInt, get: T)

object AdjoinedUnit {
  def apply[T](item: T): AdjoinedUnit[T] = new AdjoinedUnit[T](BigInt(0), item)
  implicit def ring[T](implicit ring: Ring[T]): Ring[AdjoinedUnit[T]] = new AdjoinedUnitRing[T]
}

class AdjoinedUnitRing[T](implicit ring: Ring[T]) extends Ring[AdjoinedUnit[T]] {
  val one = AdjoinedUnit[T](BigInt(1), ring.zero)
  val zero = AdjoinedUnit[T](ring.zero)

  override def isNonZero(it: AdjoinedUnit[T]) =
    (it.ones != 0) && (ring.isNonZero(it.get))

  def plus(left: AdjoinedUnit[T], right: AdjoinedUnit[T]) =
    AdjoinedUnit(left.ones + right.ones, ring.plus(left.get, right.get))

  override def negate(it: AdjoinedUnit[T]) =
    AdjoinedUnit(-it.ones, ring.negate(it.get))
  override def minus(left: AdjoinedUnit[T], right: AdjoinedUnit[T]) =
    AdjoinedUnit(left.ones - right.ones, ring.minus(left.get, right.get))

  final def intTimes(i: BigInt, v: T): T = {
    if(i < 0) {
      intTimes(i, ring.negate(v))
    }
    else if (i == 0) {
      ring.zero
    }
    else if(i == 1) {
      v
    }
    else {
      // i * v == ((i/2) * v + (i/2)*v) + (1/0)*v
      val half = i / 2
      val rem = i % 2
      val ht = intTimes(half, v)
      val twoV = ring.plus(ht, ht)
      if (rem == 0) {
        twoV
      }
      else {
        ring.plus(twoV, v)
      }
    }
  }

  def times(left: AdjoinedUnit[T], right: AdjoinedUnit[T]) = {
    // (n1, g1) * (n1, g2) = (n1*n2, (n2*g1) + (n2*g1) + g1*g2))
    val ones = left.ones * right.ones
    val part0 = intTimes(left.ones, right.get)
    val part1 = intTimes(right.ones, left.get)
    val part2 = ring.times(left.get, right.get)
    val nonUnit = ring.plus(part0, ring.plus(part1, part2))

    AdjoinedUnit(ones, nonUnit)
  }

}
