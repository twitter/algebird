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

import algebra.ring.Rng

/**
 * This is for the case where your Ring[T] is a Rng (i.e. there is no unit).
 * @see http://en.wikipedia.org/wiki/Pseudo-ring#Adjoining_an_identity_element
 */
case class AdjoinedUnit[T](ones: BigInt, get: T) {
  def unwrap: Option[T] = if (ones == 0) Some(get) else None
}

object AdjoinedUnit {
  def apply[T](item: T): AdjoinedUnit[T] = new AdjoinedUnit[T](BigInt(0), item)
  implicit def ring[T](implicit ring: Rng[T]): Ring[AdjoinedUnit[T]] =
    new AdjoinedUnitRing[T]
}

class AdjoinedUnitRing[T](implicit rng: Rng[T]) extends Ring[AdjoinedUnit[T]] {
  val one = AdjoinedUnit[T](BigInt(1), rng.zero)
  val zero = AdjoinedUnit[T](rng.zero)

  private[this] val group: Group[T] =
    new FromAlgebraGroup(rng.additive)

  override def isNonZero(it: AdjoinedUnit[T]) =
    (it.ones != 0) || group.isNonZero(it.get)

  def plus(left: AdjoinedUnit[T], right: AdjoinedUnit[T]) =
    AdjoinedUnit(left.ones + right.ones, rng.plus(left.get, right.get))

  override def negate(it: AdjoinedUnit[T]) =
    AdjoinedUnit(-it.ones, rng.negate(it.get))
  override def minus(left: AdjoinedUnit[T], right: AdjoinedUnit[T]) =
    AdjoinedUnit(left.ones - right.ones, rng.minus(left.get, right.get))

  def times(left: AdjoinedUnit[T], right: AdjoinedUnit[T]) = {
    // (n1, g1) * (n2, g2) = (n1*n2, (n1*g1) + (n2*g1) + (g1*g2))
    import Group.intTimes

    val ones = left.ones * right.ones
    val part0 = intTimes(left.ones, right.get)(group)
    val part1 = intTimes(right.ones, left.get)(group)
    val part2 = rng.times(left.get, right.get)
    val nonUnit = rng.plus(part0, rng.plus(part1, part2))

    AdjoinedUnit(ones, nonUnit)
  }

}
