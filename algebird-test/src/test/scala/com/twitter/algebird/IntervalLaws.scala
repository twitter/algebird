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

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Prop

class IntervalLaws extends PropSpec with PropertyChecks with Matchers {
  import Generators._
  import Interval.GenIntersection

  property("[x, x + 1) contains x") {
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      assert(Interval.leftClosedRightOpen(x, x + 1).contains(x))
    }
  }

  property("(x, x + 1] contains x + 1") {
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      assert(Interval.leftOpenRightClosed(x, x + 1).contains(x + 1))
    }
  }

  property("[x, x + 1) does not contain x + 1") {
    forAll { x: Int =>
      assert(!Interval.leftClosedRightOpen(x, x + 1).contains(x + 1))
    }
  }

  property("(x, x + 1] does not contain x") {
    forAll { x: Int =>
      assert(!Interval.leftOpenRightClosed(x, x + 1).contains(x))
    }
  }

  property("[x, x) is empty") {
    forAll { x: Int =>
      assert(Interval.leftClosedRightOpen(x, x).isLeft)
    }
  }

  property("(x, x] is empty") {
    forAll { x: Int =>
      assert(Interval.leftOpenRightClosed(x, x).isLeft)
    }
  }

  property("If an intersection contains, both of the intervals contain") {
    forAll { (item: Long, i1: Interval[Long], i2: Interval[Long]) =>
      assert((i1 && i2).contains(item) == (i1(item) && i2(item)))
    }
  }

  property("If an interval is empty, contains is false") {
    forAll { (item: Long, intr: Interval[Long]) =>
      intr match {
        case Empty() => assert(!intr.contains(item))
        case _ => assert(true) // may be here
      }
    }
  }

  property("[n, inf) and (-inf, n] intersect") {
    forAll { (n: Long) =>
      assert(InclusiveLower(n).intersects(InclusiveUpper(n)))
    }
  }

  property("(x, inf) and (-inf, y) intersects if and only if y > x") {
    forAll { (x: Long, y: Long) =>
      assert((y > x) == (ExclusiveLower(x).intersects(ExclusiveUpper(y))))
    }
  }

  property("(x, inf) and (-inf, y] intersect if and only if y > x") {
    forAll { (x: Long, y: Long) =>
      assert((y > x) == (ExclusiveLower(x).intersects(InclusiveUpper(y))))
    }
  }

  property("[x, inf) and (-inf, y) intersect if and only if y > x") {
    forAll { (x: Long, y: Long) =>
      assert((y > x) == (InclusiveLower(x).intersects(ExclusiveUpper(y))))
    }
  }

  property("[x, inf) and (-inf, y] intersect if and only if y >= x") {
    forAll { (x: Long, y: Long) =>
      assert((y >= x) == (InclusiveLower(x).intersects(InclusiveUpper(y))))
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
        ((low && upper) match {
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
      assert(lowerUpperIntersection(low, upper, items))
    }
  }

  // This specific case broke the tests before
  property("(n, n+1) follows the intersect law") {
    forAll { (n: Long) =>
      assert((n == Long.MaxValue) ||
        lowerUpperIntersection(ExclusiveLower(n), ExclusiveUpper(n + 1L), Nil))
    }
  }

  property("toLeftClosedRightOpen is an Injection") {
    forAll { (intr: GenIntersection[Long], tests: List[Long]) =>
      assert(intr.toLeftClosedRightOpen.map {
        case Intersection(InclusiveLower(low), ExclusiveUpper(high)) =>
          val intr2 = Interval.leftClosedRightOpen(low, high)
          tests.forall { t => intr(t) == intr2(t) }
      }.getOrElse(true)) // none means this can't be expressed as this kind of interval
    }
  }

  property("least is the smallest") {
    forAll { (lower: Lower[Long]) =>
      assert((for {
        le <- lower.least
        ple <- Predecessible.prev(le)
      } yield (lower.contains(le) && !lower.contains(ple)))
        .getOrElse {
          lower match {
            case InclusiveLower(l) => l == Long.MinValue
            case ExclusiveLower(l) => false // prev should be the lowest
          }
        })
    }
  }

  property("greatest is the biggest") {
    forAll { (upper: Upper[Long]) =>
      assert((for {
        gr <- upper.greatest
        ngr <- Successible.next(gr)
      } yield (upper.contains(gr) && !upper.contains(ngr)))
        .getOrElse {
          upper match {
            case InclusiveUpper(l) => l == Long.MaxValue
            case ExclusiveUpper(l) => false // prev should be the lowest
          }
        })
    }
  }

  property("leastToGreatest and greatestToLeast are ordered and adjacent") {
    forAll { (intr: GenIntersection[Long]) =>
      val items1 = intr.leastToGreatest.take(100)
      assert((items1.size < 2) || items1.sliding(2).forall { it =>
        it.toList match {
          case low :: high :: Nil if (low + 1L == high) => true
          case _ => false
        }
      } &&
        {
          val items2 = intr.greatestToLeast.take(100)
          (items2.size < 2) || items2.sliding(2).forall { it =>
            it.toList match {
              case high :: low :: Nil if (low + 1L == high) => true
              case _ => false
            }
          }
        })
    }
  }
}
