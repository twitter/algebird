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
 * This is a generalization of vector clocks to dynamic sizes using Hilbert spaces
 * instead of vector spaces.
 *
 * See the very readable paper:
 * http://gsd.di.uminho.pt/members/cbm/ps/itc2008.pdf
 */
object IntervalTree {

  /**
   * This is like the index in the vector clock for each of the nodes
   */
  sealed trait Id {
    def apply(x: Double): Boolean
    def depth: Int
    def normalize: Id
    /**
     * This returns a pair of ids such that
     * left + right == this
     * left * right = ZeroId
     */
    def split: (Id, Id)
  }
  case object OneId extends Id {
    def apply(x: Double) = (x < 1.0) && (x >= 0.0)
    def depth = 1
    def normalize = this
    def split = (TreeId(OneId, ZeroId), TreeId(ZeroId, OneId))
  }
  case object ZeroId extends Id {
    def apply(x: Double) = false
    def depth = 1
    def normalize = this
    def split = (ZeroId, ZeroId)
  }
  case class TreeId(left: Id, right: Id) extends Id {
    def apply(x: Double) = {
      val twox = 2.0 * x
      left(twox) || right(twox - 1.0)
    }
    def depth = 1 + math.max(left.depth, right.depth)
    def normalize = (left, right) match {
      case (OneId, OneId) => OneId
      case (ZeroId, ZeroId) => ZeroId
      case _ => this
    }

    def split = (left, right) match {
      case (ZeroId, r) =>
        val (r0, r1) = r.split
        (TreeId(ZeroId, r0), TreeId(ZeroId, r1))
      case (l, ZeroId) =>
        val (l0, l1) = l.split
        (TreeId(l0, ZeroId), TreeId(l1, ZeroId))
      case (l, r) => (TreeId(l, ZeroId), TreeId(ZeroId, r))
    }
  }
  object Id {
    implicit val equiv: Equiv[Id] = Equiv.fromFunction[Id](_.normalize == _.normalize)
    implicit val idRing: Ring[Id] = new Ring[Id] {
      def zero = ZeroId
      def one = OneId
      override def negate(t: Id) = sys.error("This is a semiring")
      override def isNonZero(that: Id) = that.normalize != zero
      // This is actually a Band x + x == x
      def plus(left: Id, right: Id) = (left, right) match {
        case (ZeroId, r) => r
        case (l, ZeroId) => l
        case (OneId, _) => OneId
        case (_, OneId) => OneId
        case (TreeId(ll, lr), TreeId(rl, rr)) =>
          TreeId(plus(ll, rl), plus(lr, rr)).normalize
      }
      // This is actually a Band x * x == x
      def times(left: Id, right: Id) = (left, right) match {
        case (ZeroId, _) => ZeroId
        case (_, ZeroId) => ZeroId
        case (OneId, r) => r
        case (l, OneId) => l
        case (TreeId(ll, lr), TreeId(rl, rr)) =>
          TreeId(times(ll, rl), times(lr, rr)).normalize
      }
    }
  }
  /**
   * This is like the vector is a vector clock, but it
   * is in an infinite dimensional space which, by normalization
   * and some minimization tricks, the size is kept under control
   */
  sealed trait Event {
    def apply(x: Double): Long
    /**
     * How deep is the tree that represents this event
     */
    def depth: Int
    def lift(m: Long): Event
    def sink(m: Long): Event
    def min: Long
    def max: Long
    def normalize: Event
    def maxEvent: ConstEvent = ConstEvent(max)
    def minEvent: ConstEvent = ConstEvent(min)
  }

  object Event {
    implicit def equiv: Equiv[Event] = Equiv.fromFunction[Event](_.normalize == _.normalize)
    /**
     * This is called join in the paper in section 5.3.3
     * This operation is the pointwise maximum of the Event function
     * Note, this is a band, in that it is idempotent
     */
    implicit val monoid: Monoid[Event] = new Monoid[Event] {
      private def toTree(c: ConstEvent): TreeEvent =
        TreeEvent(c.height, ConstEvent(0L), ConstEvent(0L))

      val zero = ConstEvent(0L)

      def plus(left: Event, right: Event) = (left, right) match {
        case (ConstEvent(l), ConstEvent(r)) => ConstEvent(l max r)
        case (c @ ConstEvent(_), r) => plus(toTree(c), r)
        case (l, c @ ConstEvent(_)) => plus(l, toTree(c))
        case (l @ TreeEvent(lh, _, _), r @ TreeEvent(rh, _, _)) if (lh > rh) => plus(r, l)
        case (TreeEvent(lh, ll, lr), TreeEvent(rh, rl, rr)) =>
          val gap = rh - lh // This must be positive due to the prior case
          TreeEvent(lh, plus(ll, rl.lift(gap)), plus(lr, rr.lift(gap))).normalize
      }
    }
    /**
     * e1 <= e2 if the e2(x) >= e1(x) for all x
     */
    implicit val partialOrd: PartialOrdering[Event] = new PartialOrdering[Event] {
      // see section 5.3.1 of the paper
      def lteq(left: Event, right: Event): Boolean = lteqRec(left.normalize, right.normalize)

      // This one does not call normalize each recursion as only once is needed
      private def lteqRec(left: Event, right: Event): Boolean =
        (left, right) match {
          case (ConstEvent(l), ConstEvent(r)) => l <= r
          case (ConstEvent(lh), TreeEvent(rh, _, _)) => lh <= rh
          case (TreeEvent(lh, ltree, rtree), rconst @ ConstEvent(rh)) =>
            (lh <= rh) &&
              lteqRec(ltree.lift(lh), rconst) &&
              lteqRec(rtree.lift(lh), rconst)
          case (TreeEvent(lh, lltree, lrtree), TreeEvent(rh, rltree, rrtree)) =>
            (lh <= rh) &&
              lteqRec(lltree.lift(lh), rltree.lift(rh)) &&
              lteqRec(lrtree.lift(lh), rrtree.lift(rh))
        }
      def tryCompare(left: Event, right: Event): Option[Int] = {
        val ln = left.normalize
        val rn = right.normalize
        if (ln == rn) Some(0)
        else if (lteqRec(ln, rn)) Some(-1)
        else if (lteqRec(rn, ln)) Some(1)
        else None
      }
    }
  }

  case class ConstEvent(height: Long) extends Event {
    require(height >= 0, "height must be non-negative")
    def apply(x: Double) = if ((x < 1.0) && (x >= 0.0)) height else 0L
    def depth = 1
    def lift(m: Long) = ConstEvent(height + m)
    def sink(m: Long) = ConstEvent(height - m)
    def min = height
    def max = height
    def normalize = this
  }
  case class TreeEvent(height: Long, left: Event, right: Event) extends Event {
    require(height >= 0, "height must be non-negative")
    def apply(x: Double) =
      if ((x < 1.0) && (x >= 0.0)) {
        val tx = 2.0 * x
        height + left(tx) + right(tx - 1.0)
      } else 0L

    def depth = math.max(left.depth, right.depth) + 1
    def lift(m: Long) = TreeEvent(height + m, left, right)
    def sink(m: Long) = TreeEvent(height - m, left, right)
    private lazy val childrenMin = math.min(left.min, right.min)
    def min = height + childrenMin
    def max = height + math.max(left.max, right.max)
    def normalize = {
      val cmin = childrenMin
      val leftn = left.sink(cmin)
      val rightn = right.sink(cmin)
      (leftn, rightn) match {
        case (ConstEvent(m1), ConstEvent(m2)) if (m1 == m2) => ConstEvent(height + m1 + cmin)
        case (l, r) => TreeEvent(height + cmin, l, r)
      }
    }
  }

  object Stamp {
    /**
     * We compare Stamps purely on the basis of the event
     */
    implicit val partialOrdering: PartialOrdering[Stamp] = new PartialOrdering[Stamp] {
      def lteq(l: Stamp, r: Stamp) = (l, r) match {
        case (Stamp(_, le), Stamp(_, re)) =>
          Event.partialOrd.lteq(le, re)
      }
      def tryCompare(l: Stamp, r: Stamp) = (l, r) match {
        case (Stamp(_, le), Stamp(_, re)) =>
          Event.partialOrd.tryCompare(le, re)
      }
    }
    implicit val equiv: Equiv[Stamp] = Equiv.fromFunction {
      case (Stamp(lid, lev), Stamp(rid, rev)) =>
        Equiv[Id].equiv(lid, rid) && Equiv[Event].equiv(lev, rev)
    }
    implicit val succ: Successible[Stamp] = new Successible[Stamp] {
      def next(s: Stamp) = s.next
      def partialOrdering = Stamp.partialOrdering
    }
    implicit val monoid: Monoid[Stamp] = new Monoid[Stamp] {
      def zero = Stamp(ZeroId, ConstEvent(0L))
      def plus(l: Stamp, r: Stamp) = l.join(r)
    }
    implicit val clock: Clock[Stamp] = new Clock[Stamp] {
      def successible = Stamp.succ
      def semilattice = monoid
    }
  }

  /**
   * This is the initial value of the time Stamp.
   * Start here and forkMany to create a bunch of
   * Stamps across several nodes. Any node can
   * fork to add new nodes to the system.
   */
  val initial: Stamp = Stamp(OneId, ConstEvent(0L))

  /*
   * This is the type that is used to make an IntervalTree clock
   * The Id represents the part of the vector space (called a support)
   * where the current node can increment the Event.
   */
  case class Stamp(id: Id, event: Event) {
    // If id != Zero this is always defined
    def next: Option[Stamp] = id match {
      case ZeroId => None
      case _ =>
        val e1 = fill
        Some(if (e1 != event) Stamp(id, e1)
        else Stamp(id, grow))
    }

    def fill: Event = (id, event) match {
      case (ZeroId, e) => e
      case (OneId, e) => e.maxEvent
      case (_, c @ ConstEvent(_)) => c
      case (TreeId(OneId, rid), TreeEvent(h, l, r)) =>
        val rightFilled = Stamp(rid, r).fill
        TreeEvent(h, ConstEvent(math.max(l.max, rightFilled.min)), rightFilled)
          .normalize
      case (TreeId(lid, OneId), TreeEvent(h, l, r)) =>
        val leftFilled = Stamp(lid, l).fill
        TreeEvent(h, leftFilled, ConstEvent(math.max(r.max, leftFilled.min)))
          .normalize
      case (TreeId(lid, rid), TreeEvent(h, l, r)) =>
        TreeEvent(h, Stamp(lid, l).fill, Stamp(rid, r).fill)
          .normalize
    }

    /**
     * When we want to add nodes into the communication system,
     * we fork the current state. fork then join is identity
     */
    def fork: (Stamp, Stamp) = {
      val (lid, rid) = id.split
      (Stamp(lid, event), Stamp(rid, event))
    }
    /**
     * ways must be greater than 0
     */
    def forkMany(ways: Int): Seq[Stamp] = ways match {
      case x if x < 1 => sys.error("Cannot fork negative times: " + x)
      case 1 => Seq(this)
      case x =>
        val leftCount = ways / 2
        val (left, right) = fork
        left.forkMany(leftCount) ++ right.forkMany(ways - leftCount)
    }

    def grow: Event = {
      val costOrd: Ordering[(Int, Int)] = Ordering.by {
        case (l, r) =>
          // First take the minimum total cost, then min diff, then prefer right
          val diff = l - r
          (l + r, math.abs(diff), if (diff > 0) 1 else -1)
      }
      def grow(i: Id, ev: Event): (Event, (Int, Int)) = (i, ev) match {
        case (ZeroId, _) => sys.error("Cannot grow on the zero support")
        case (OneId, ConstEvent(n)) => (ConstEvent(n + 1L), (0, 0))
        case (OneId, te @ TreeEvent(_, _, _)) => grow(TreeId(OneId, OneId), te)
        case (t @ TreeId(_, _), ConstEvent(n)) => grow(t, TreeEvent(n, ConstEvent(0L), ConstEvent(0L)))
        case (TreeId(ZeroId, r), TreeEvent(h, le, re)) =>
          val (re1, (cl, cr)) = grow(r, re)
          (TreeEvent(h, le, re1), (cl, cr + 1))
        case (TreeId(l, ZeroId), TreeEvent(h, le, re)) =>
          val (le1, (cl, cr)) = grow(l, le)
          (TreeEvent(h, le1, re), (cl + 1, cr))
        case (TreeId(l, r), TreeEvent(h, le, re)) =>
          // Todo: We might want to memoize this? benchmark
          val (re1, rc @ (rcl, rcr)) = grow(r, re)
          val (le1, lc @ (lcl, lcr)) = grow(l, le)
          if (costOrd.lteq(rc, lc)) {
            (TreeEvent(h, le, re1), (rcl, rcr + 1))
          } else {
            (TreeEvent(h, le1, re), (lcl + 1, lcr))
          }
      }
      grow(id, event)._1
    }

    def join(that: Stamp): Stamp = that match {
      case Stamp(rid, rev) => Stamp(Semigroup.plus(id, rid), Semigroup.plus(event, rev))
    }
  }
}

