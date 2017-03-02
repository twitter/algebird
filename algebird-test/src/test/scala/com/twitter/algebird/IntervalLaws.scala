/*
Copyright 2013 Twitter, Inc.

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

import org.scalacheck.Prop._
import com.twitter.algebird.scalacheck.PosNum

class IntervalLaws extends CheckProperties {
  import com.twitter.algebird.scalacheck.arbitrary._
  import com.twitter.algebird.Interval.GenIntersection

  property("mapNonDecreasing preserves contains behavior") {
    forAll { (interval: Interval[Int], t: Int, inc: PosNum[Int]) =>
      def nonDecreasing(i: Int): Long = i.toLong + inc.value

      val mappedInterval: Interval[Long] = interval.mapNonDecreasing(nonDecreasing(_))
      val mappedT: Long = nonDecreasing(t)

      interval.contains(t) == mappedInterval.contains(mappedT)
    }
  }

  property("[x, x + 1) contains x") {
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      Interval.leftClosedRightOpen(x, x + 1).contains(x)
    }
  }

  property("[x, x + 1] contains x, x+1") {
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      val intr = Interval.closed(x, x + 1)
      intr.contains(x) &&
        intr.contains(x + 1) &&
        (!intr.contains(x + 2)) &&
        (!intr.contains(x - 1))
    }
  }
  property("(x, x + 2) contains x+1") {
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      val intr = Interval.open(x, x + 2)
      intr.contains(x + 1) &&
        (!intr.contains(x + 2)) &&
        (!intr.contains(x))
    }
  }

  property("(x, x + 1] contains x + 1") {
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      Interval.leftOpenRightClosed(x, x + 1).contains(x + 1)
    }
  }

  property("[x, x + 1) does not contain x + 1") {
    forAll { x: Int =>
      !Interval.leftClosedRightOpen(x, x + 1).contains(x + 1)
    }
  }

  property("(x, x + 1] does not contain x") {
    forAll { x: Int =>
      !Interval.leftOpenRightClosed(x, x + 1).contains(x)
    }
  }

  property("[x, x) is empty") {
    forAll { x: Int =>
      Interval.leftClosedRightOpen(x, x).isEmpty
    }
  }

  property("(x, x] is empty") {
    forAll { x: Int =>
      Interval.leftOpenRightClosed(x, x).isEmpty
    }
  }

  property("If an intersection contains, both of the intervals contain") {
    forAll { (item: Long, i1: Interval[Long], i2: Interval[Long]) =>
      (i1 && i2).contains(item) == (i1(item) && i2(item))
    }
  }

  property("If an interval is empty, contains is false") {
    forAll { (item: Long, intr: Interval[Long]) =>
      intr match {
        case Empty() => !intr.contains(item)
        case _ => true // may be here
      }
    }
  }

  property("[n, inf) and (-inf, n] intersect") {
    forAll { (n: Long) =>
      InclusiveLower(n).intersects(InclusiveUpper(n))
    }
  }

  property("(x, inf) and (-inf, y) intersects if and only if y > x") {
    forAll { (x: Long, y: Long) =>
      ((y > x) == ExclusiveLower(x).intersects(ExclusiveUpper(y)))
    }
  }

  property("(x, inf) and (-inf, y] intersect if and only if y > x") {
    forAll { (x: Long, y: Long) =>
      ((y > x) == ExclusiveLower(x).intersects(InclusiveUpper(y)))
    }
  }

  property("[x, inf) and (-inf, y) intersect if and only if y > x") {
    forAll { (x: Long, y: Long) =>
      ((y > x) == InclusiveLower(x).intersects(ExclusiveUpper(y)))
    }
  }

  property("[x, inf) and (-inf, y] intersect if and only if y >= x") {
    forAll { (x: Long, y: Long) =>
      ((y >= x) == InclusiveLower(x).intersects(InclusiveUpper(y)))
    }
  }

  def lowerUpperIntersection(low: Lower[Long], upper: Upper[Long], items: List[Long]) = {
    if (low.intersects(upper)) {
      low.least.map { lb =>
        // This is the usual case
        upper.contains(lb) || {
          // but possibly we have: (lb, lb+1)
          Equiv[Option[Long]].equiv(Some(lb), upper.strictUpperBound)
        }
      }.getOrElse(true) &&
        (low && upper match {
          case Intersection(_, _) => true
          case _ => false
        })
    } else {
      // nothing is in both
      low.least.map(upper.contains(_) == false).getOrElse(true) &&
        items.forall { i => (low.contains(i) && upper.contains(i)) == false } &&
        (low && upper match {
          case Empty() => true
          case _ => false
        })
    }
  }
  property("If an a Lower intersects an Upper, the intersection is non Empty") {
    forAll { (low: Lower[Long], upper: Upper[Long], items: List[Long]) =>
      (lowerUpperIntersection(low, upper, items))
    }
  }

  // This specific case broke the tests before
  property("(n, n+1) follows the intersect law") {
    forAll { (n: Long) =>
      ((n == Long.MaxValue) ||
        lowerUpperIntersection(ExclusiveLower(n), ExclusiveUpper(n + 1L), Nil))
    }
  }

  property("toLeftClosedRightOpen is an Injection") {
    forAll { (intr: GenIntersection[Long], tests: List[Long]) =>
      (intr.toLeftClosedRightOpen.map {
        case Intersection(InclusiveLower(low), ExclusiveUpper(high)) =>
          val intr2 = Interval.leftClosedRightOpen(low, high)
          tests.forall { t => intr(t) == intr2(t) }
      }.getOrElse(true)) // none means this can't be expressed as this kind of interval
    }
  }

  property("least is the smallest") {
    forAll { (lower: Lower[Long]) =>
      ((for {
        le <- lower.least
        ple <- Predecessible.prev(le)
      } yield lower.contains(le) && !lower.contains(ple))
        .getOrElse {
          lower match {
            case InclusiveLower(l) => l == Long.MinValue
            case ExclusiveLower(l) => l == Long.MaxValue
          }
        })
    }
  }

  property("greatest is the biggest") {
    forAll { (upper: Upper[Long]) =>
      ((for {
        gr <- upper.greatest
        ngr <- Successible.next(gr)
      } yield upper.contains(gr) && !upper.contains(ngr))
        .getOrElse {
          upper match {
            case InclusiveUpper(l) => l == Long.MaxValue
            case ExclusiveUpper(l) => l == Long.MinValue
          }
        })
    }
  }

  property("leastToGreatest and greatestToLeast are ordered and adjacent") {
    forAll { (intr: GenIntersection[Long]) =>
      val items1 = intr.leastToGreatest.take(100)
      ((items1.size < 2) || items1.sliding(2).forall { it =>
        it.toList match {
          case low :: high :: Nil if low + 1L == high => true
          case _ => false
        }
      } &&
        {
          val items2 = intr.greatestToLeast.take(100)
          (items2.size < 2) || items2.sliding(2).forall { it =>
            it.toList match {
              case high :: low :: Nil if low + 1L == high => true
              case _ => false
            }
          }
        })
    }
  }

  property("if Interval.isEmpty then it contains nothing") {
    forAll { (intr: Interval[Long], i: Long, rest: List[Long]) =>
      if (intr.isEmpty) {
        (i :: rest).exists(intr(_)) == false
      } else true
    }
  }
  property("nothing is smaller than least") {
    forAll { (intr: Interval[Long], i: Long, rest: List[Long]) =>
      intr.boundedLeast match {
        case Some(l) => (i :: rest).forall { v =>
          !intr(v) || (l <= v)
        }
        case None => true
      }
    }
  }
  property("nothing is bigger than greatest") {
    forAll { (intr: Interval[Long], i: Long, rest: List[Long]) =>
      intr.boundedGreatest match {
        case Some(u) => (i :: rest).forall { v =>
          !intr(v) || (v <= u)
        }
        case None => true
      }
    }
  }
  property("(a && b).boundedLeast >= max(a.boundedLeast, b.boundedLeast)") {
    forAll { (a: Interval[Long], b: Interval[Long]) =>
      (a && b).boundedLeast match {
        case Some(l) =>
          (a.boundedLeast, b.boundedLeast) match {
            case (Some(v), None) => v == l
            case (None, Some(v)) => v == l
            case (Some(v1), Some(v2)) => l == math.max(v1, v2)
            case (None, None) => false // should never happen
          }
        case None => true
      }
    }
  }
  property("(a && b).boundedGreatest <= max(a.boundedGreatest, b.boundedGreatest)") {
    forAll { (a: Interval[Long], b: Interval[Long]) =>
      (a && b).boundedGreatest match {
        case Some(l) =>
          (a.boundedGreatest, b.boundedGreatest) match {
            case (Some(v), None) => v == l
            case (None, Some(v)) => v == l
            case (Some(v1), Some(v2)) => l == math.min(v1, v2)
            case (None, None) => false // should never happen
          }
        case None => true
      }
    }
  }
  property("if boundedLeast is none, we are Universe, Upper or isEmpty is true") {
    forAll { (a: Interval[Long]) =>
      a.boundedLeast match {
        case Some(_) => true
        case None => a.isEmpty || (a match {
          case _: Upper[_] => true
          case Universe() => true
          case _ => false
        })
      }
    }
  }
  property("if boundedGreatest is none, we are Universe, Lower or isEmpty is true") {
    forAll { (a: Interval[Long]) =>
      a.boundedGreatest match {
        case Some(_) => true
        case None => a.isEmpty || (a match {
          case _: Lower[_] => true
          case Universe() => true
          case _ => false
        })
      }
    }
  }
}
