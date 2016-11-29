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

object VectorClock {
  /**
   * This is the timestamp for a vector clock. It is a slight
   * generalization that allows one stamp to own more than
   * one id. This is to implement the join (the semilattice operation)
   */
  case class Stamp(ids: Set[Int], clock: Vector[Long]) {
    def next: Option[Stamp] =
      ids.find { clock(_) != Long.MaxValue }
        .map { id =>
          val oldC = clock(id) // This is not the maximum
          Stamp(ids, clock.updated(id, oldC + 1L))
        }

    /**
     * If this stamp has more than one id, it can remove one to share
     * with another node. The first value is the stamp with the given id
     */
    def fork(id: Int): Option[(Stamp, Stamp)] =
      if (ids(id)) Some(Stamp(Set(id), clock), Stamp(ids - id, clock))
      else None

    def forkAll: Set[Stamp] = ids.map { i => Stamp(Set(i), clock) }

    def join(that: Stamp): Stamp = that match {
      case Stamp(thatIds, thatClock) =>
        Stamp(ids ++ thatIds, padZip(0L, clock, thatClock).map {
          case (l, r) =>
            math.max(l, r)
        })
    }
  }

  private def padZip(pad: Long, left: Vector[Long], right: Vector[Long]): Vector[(Long, Long)] = (left, right) match {
    case (l, r) if l.size < r.size => padZip(pad, r, l)
    case (l, r) if l.size > r.size => l.zip(r.padTo(r.size, pad))
    case (l, r) => l.zip(r)
  }

  object Stamp {
    implicit val partialOrdering: PartialOrdering[Stamp] = new PartialOrdering[Stamp] {
      def lteq(left: Stamp, right: Stamp) =
        padZip(0L, left.clock, right.clock).forall {
          case (l, r) =>
            l <= r
        }
      def tryCompare(left: Stamp, right: Stamp) = {
        val zipped = padZip(0L, left.clock, right.clock)
        if (zipped.forall { case (l, r) => l < r }) Some(-1)
        else if (zipped.forall { case (l, r) => l == r }) Some(0)
        else if (zipped.forall { case (l, r) => l >= r }) Some(1)
        else None
      }
    }
    implicit val equiv: Equiv[Stamp] = new Equiv[Stamp] {
      def equiv(left: Stamp, right: Stamp) = (left.ids == right.ids) &&
        padZip(0L, left.clock, right.clock).forall { case (l, r) => (l == r) }
    }
    implicit val successible: Successible[Stamp] = new Successible[Stamp] {
      def next(s: Stamp) = s.next
      def partialOrdering = Stamp.partialOrdering
    }
    implicit val monoid: Monoid[Stamp] = new Monoid[Stamp] {
      def zero = Stamp(Set.empty, Vector.empty)
      def plus(left: Stamp, right: Stamp) = left.join(right)
    }
    implicit val clock: Clock[Stamp] = new Clock[Stamp] {
      def successible = Stamp.successible
      def semilattice = Stamp.monoid
    }
  }

  /**
   * Initializes a stamp for the given node id
   */
  def initialize(id: Int): Stamp = Stamp(Set(id), Vector.fill(id + 1)(0L))
}
