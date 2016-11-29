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

package com.twitter.algebird.clock

import com.twitter.algebird._

/**
 * This is a type-class for a type that can be used as a clock
 * Clocks need to be incrementable, have a partial ordering,
 * and have way to get the upperbound of two clocks (semilattice)
 *
 * With a clock, we can make a partial-semigroup
 */
trait Clock[T] {
  def successible: Successible[T]
  /**
   * This should be a commutative, idempotent, semigroup
   * which is called a semilattice:
   * http://en.wikipedia.org/wiki/Semilattices
   *
   * TODO: add types for commutative and idempotent
   */
  def semilattice: Semigroup[T]
  final def partialOrdering: PartialOrdering[T] = successible.partialOrdering
}

object Clock {
  /*
   * These are trivial clocks which can be useful for single node
   * contexts
   */
  implicit def integralClock[T: Integral]: Clock[T] = new Clock[T] {
    def successible = implicitly[Successible[T]]
    def semilattice = Semigroup.from(implicitly[Integral[T]].max(_, _))
  }

  /**
   * This only folds when the newer item is strictly greater
   * than the older item (keeping in mind, clocks only have
   * partial orderings). This returns None with the clock
   * values are incomparable, equal or the right value is smaller
   */
  def foldGT[C, T, U, V](older: (C, T),
    newer: (C, U))(fn: (T, U) => V)(implicit c: Clock[C]): Option[(C, V)] = {
    val (nc, nu) = newer
    val (oc, ot) = older
    c.partialOrdering.tryCompare(oc, nc) match {
      case Some(x) if x < 0 => Some((c.semilattice.plus(oc, nc), fn(ot, nu)))
      case _ => None
    }
  }
  /**
   * This only folds when the newer item clock is not less
   * than the older item clock (keeping in mind, clocks only have
   * partial orderings). This includes cases where the clocks are
   * not comparable, the clocks are equal, and right value is newer.
   */
  def foldNotLT[C, T, U, V](older: (C, T),
    newer: (C, U))(fn: (T, U) => V)(implicit c: Clock[C]): Option[(C, V)] = {
    val (nc, nu) = newer
    val (oc, ot) = older
    c.partialOrdering.tryCompare(nc, oc) match {
      case Some(x) if x < 0 => None
      case _ => Some((c.semilattice.plus(oc, nc), fn(ot, nu)))
    }
  }
  /**
   * This only folds when the newer item clock is not less than or equal
   * the older item clock (keeping in mind, clocks only have
   * partial orderings). This includes cases where the clocks are
   * not comparable, the clocks are equal, and right value is newer.
   */
  def foldNotLTEQ[C, T, U, V](older: (C, T),
    newer: (C, U))(fn: (T, U) => V)(implicit c: Clock[C]): Option[(C, V)] = {
    val (nc, nu) = newer
    val (oc, ot) = older
    c.partialOrdering.tryCompare(nc, oc) match {
      case None => Some((c.semilattice.plus(oc, nc), fn(ot, nu)))
      case Some(x) if x > 0 => Some((c.semilattice.plus(oc, nc), fn(ot, nu)))
      case Some(x) if x <= 0 => None
    }
  }

  /**
   * Try to increment the time in the tuple. If the clock
   * has a maximum (e.g. C == Int), this can return None
   */
  def nextTime[C, T](c: (C, T))(implicit clk: Clock[C]): Option[(C, T)] =
    clk.successible.next(c._1).map((_, c._2))

  /**
   * This is a PartialSemigroup that is only defined if the right
   * is not less than of equal the left-hand side.
   * This is merges new and incomparable clocks using the semigroup.
   * The semigroup is being used to resolve conflicts here.
   */
  def notLTEQSemigroup[C, T](implicit c: Clock[C], m: Semigroup[T]): PartialSemigroup[(C, T)] =
    new PartialSemigroup[(C, T)] {
      def tryPlus(older: (C, T), newer: (C, T)) =
        foldNotLTEQ(older, newer)(m.plus)
    }
  /**
   * This is a PartialSemigroup that is only defined if the right
   * is greater than the left-hand side.
   * This is merges only new items, and rejects incomparable clocks,
   * or clocks in the past.
   * The semigroup is NOT being used to resolve conflicts here.
   */
  def isGTSemigroup[C, T](implicit c: Clock[C], m: Semigroup[T]): PartialSemigroup[(C, T)] =
    new PartialSemigroup[(C, T)] {
      def tryPlus(older: (C, T), newer: (C, T)) =
        foldGT(older, newer)(m.plus)
    }

  /**
   * Change the value and increment the clock if there is a larger value
   */
  def mapNext[C, T, U](stamped: (C, T))(fn: T => U)(implicit clock: Clock[C]): Option[(C, U)] = {
    val (c, t) = stamped
    clock.successible.next(c).map { (_, fn(t)) }
  }
}
