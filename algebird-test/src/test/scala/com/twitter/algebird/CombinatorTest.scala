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

import org.scalacheck.Arbitrary

class CombinatorTest extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit def minArb[T: Arbitrary]: Arbitrary[Min[T]] = Arbitrary {
    Arbitrary.arbitrary[T].map { t => Min(t) }
  }
  implicit def maxArb[T: Arbitrary]: Arbitrary[Max[T]] = Arbitrary {
    Arbitrary.arbitrary[T].map { t => Max(t) }
  }

  implicit val sg: HasAdditionOperator[(Max[Int], List[Int])] =
    new HasAdditionOperatorCombinator({ (m: Max[Int], l: List[Int]) =>
      val sortfn = { (i: Int) => i % (scala.math.sqrt(m.get.toLong - Int.MinValue).toInt + 1) }
      l.sortWith { (l, r) =>
        val (sl, sr) = (sortfn(l), sortfn(r))
        if (sl == sr) l < r else sl < sr
      }
    })

  implicit val mond: HasAdditionOperatorAndZero[(Max[Int], List[Int])] =
    new HasAdditionOperatorAndZeroCombinator({ (m: Max[Int], l: List[Int]) =>
      val sortfn = { (i: Int) => i % (scala.math.sqrt(m.get.toLong - Int.MinValue).toInt + 1) }
      l.sortWith { (l, r) =>
        val (sl, sr) = (sortfn(l), sortfn(r))
        if (sl == sr) l < r else sl < sr
      }
    })
  // Make sure the lists start sorted:
  implicit def pairArb(implicit lista: Arbitrary[List[Int]]): Arbitrary[(Max[Int], List[Int])] =
    Arbitrary {
      for (
        m <- Arbitrary.arbitrary[Max[Int]];
        l <- Arbitrary.arbitrary[List[Int]]
      ) yield mond.plus(mond.zero, (m, l))
    }

  property("HasAdditionOperatorCombinator with mod sortfn forms a HasAdditionOperator") {
    semigroupLaws[(Max[Int], List[Int])]
  }

  property("HasAdditionOperatorAndZeroCombinator with mod sortfn forms a HasAdditionOperatorAndZero") {
    monoidLaws[(Max[Int], List[Int])]
  }

  // Now test the expected use case: top-K by appearances:
  implicit val monTopK: HasAdditionOperatorAndZero[(Map[Int, Int], Set[Int])] =
    new HasAdditionOperatorAndZeroCombinator({ (m: Map[Int, Int], top: Set[Int]) =>
      top.toList.sortWith { (l, r) =>
        val lc = m(l)
        val rc = m(r)
        if (lc == rc) l > r else lc > rc
        // Probably only approximately true with this cut-off
      }.take(40).toSet
    })
  // Make sure the sets start sorted:
  implicit def topKArb: Arbitrary[(Map[Int, Int], Set[Int])] =
    Arbitrary {
      for (
        s <- Arbitrary.arbitrary[List[Int]];
        smallvals = s.map { _ % 31 };
        m = smallvals.groupBy { s => s }.mapValues { _.size }
      ) yield monTopK.plus(monTopK.zero, (m, smallvals.toSet))
    }
  property("HasAdditionOperatorAndZeroCombinator with top-K forms a HasAdditionOperatorAndZero") {
    monoidLaws[(Map[Int, Int], Set[Int])]
  }

}
