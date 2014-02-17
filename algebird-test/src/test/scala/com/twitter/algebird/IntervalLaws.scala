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

import org.scalacheck.Properties
import org.scalacheck.Prop._

object IntervalLaws extends Properties("Interval") {
  import Generators._

  property("[x, x + 1) contains x") =
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      Interval.leftClosedRightOpen(x, x + 1).contains(x)
    }

  property("(x, x + 1] contains x + 1") =
    forAll { y: Int =>
      val x = y.asInstanceOf[Long]
      Interval.leftOpenRightClosed(x, x + 1).contains(x + 1)
    }

  property("[x, x + 1) does not contain x + 1") =
    forAll { x: Int => ! Interval.leftClosedRightOpen(x, x + 1).contains(x + 1) }

  property("(x, x + 1] does not contain x") =
    forAll { x: Int => ! Interval.leftOpenRightClosed(x, x + 1).contains(x) }

  property("[x, x) is empty") =
    forAll { x : Int => Interval.leftClosedRightOpen(x, x) == Empty[Int]() }

  property("If an intersection contains, both of the intervals contain") =
    forAll { (item: Long, i1: Interval[Long], i2: Interval[Long]) =>
      (i1 && i2).contains(item) == (i1(item) && i2(item))
    }

  property("least is the smallest") =
    forAll { (lower: Lower[Long]) =>
      (for {
        le <- lower.least
        nle <- Successible.next(le)
        ple <- Predecessible.prev(le)
      } yield (lower.contains(nle) && !lower.contains(ple)))
        .getOrElse {
          lower match {
            case InclusiveLower(l) => l == Long.MinValue
            case ExclusiveLower(l) => false // prev should be the lowest
          }
        }
    }

  property("greatest is the biggest") =
    forAll { (upper: Upper[Long]) =>
      (for {
        gr <- upper.greatest
        ngr <- Successible.next(gr)
        pgr <- Predecessible.prev(gr)
      } yield (upper.contains(pgr) && !upper.contains(ngr)))
        .getOrElse {
          upper match {
            case InclusiveUpper(l) => l == Long.MaxValue
            case ExclusiveUpper(l) => false // prev should be the lowest
          }
        }
    }

  property("leastToGreatest and greatestToLeast are ordered and adjacent") =
    forAll { (intr: Intersection[Long]) =>
      val items1 = intr.leastToGreatest.take(100)
      (items1.size < 2) || items1.sliding(2).forall { it =>
        it.toList match {
          case low::high::Nil if (low + 1L == high) => true
          case _ => false
        }
      } &&
      { val items2 = intr.greatestToLeast.take(100)
        (items2.size < 2) || items2.sliding(2).forall { it =>
          it.toList match {
            case high::low::Nil if (low + 1L == high) => true
            case _ => false
          }
        }
      }
    }
}
