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
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

object CombinatorTest extends Properties("Combinator") {
  import BaseProperties._

  implicit def minArb[T:Arbitrary]: Arbitrary[Min[T]] = Arbitrary {
    Arbitrary.arbitrary[T].map { t => Min(t) } }
  implicit def maxArb[T:Arbitrary]: Arbitrary[Max[T]] = Arbitrary {
    Arbitrary.arbitrary[T].map { t => Max(t) } }

  implicit val sg: Semigroup[(Max[Int],List[Int])] =
    new SemigroupCombinator({ (m: Max[Int], l: List[Int]) =>
      val sortfn = {(i: Int) => i % (scala.math.sqrt(m.get.toLong - Int.MinValue).toInt + 1)}
      l.sortWith { (l, r) =>
        val (sl, sr) = (sortfn(l), sortfn(r))
        if(sl == sr) l < r else sl < sr
      }
    })

  implicit val mond: Monoid[(Max[Int],List[Int])] =
    new MonoidCombinator({(m: Max[Int], l: List[Int]) =>
      val sortfn = {(i: Int) => i % (scala.math.sqrt(m.get.toLong - Int.MinValue).toInt + 1)}
      l.sortWith { (l, r) =>
        val (sl, sr) = (sortfn(l), sortfn(r))
        if(sl == sr) l < r else sl < sr
      }
    })
  // Make sure the lists start sorted:
  implicit def pairArb(implicit lista: Arbitrary[List[Int]]): Arbitrary[(Max[Int], List[Int])] =
    Arbitrary {
      for(m <- Arbitrary.arbitrary[Max[Int]];
          l <- Arbitrary.arbitrary[List[Int]]) yield mond.plus(mond.zero, (m,l))
    }

  property("SemigroupCombinator with mod sortfn forms a Semigroup") = semigroupLaws[(Max[Int],List[Int])]
  property("MonoidCombinator with mod sortfn forms a Monoid") = monoidLaws[(Max[Int],List[Int])]

  // Now test the expected use case: top-K by appearances:
  implicit val monTopK: Monoid[(Map[Int, Int], Set[Int])] =
    new MonoidCombinator({(m: Map[Int, Int], top: Set[Int]) =>
      top.toList.sortWith {(l,r) =>
        val lc = m(l)
        val rc = m(r)
        if(lc == rc) l > r else lc > rc
        // Probably only approximately true with this cut-off
      }.take(40).toSet
    })
  // Make sure the sets start sorted:
  implicit def topKArb: Arbitrary[(Map[Int, Int], Set[Int])] =
    Arbitrary {
      for(s <- Arbitrary.arbitrary[List[Int]];
          smallvals = s.map { _ % 31 };
          m = smallvals.groupBy { s => s }.mapValues { _.size }) yield monTopK.plus(monTopK.zero, (m,smallvals.toSet))
    }
  property("MonoidCombinator with top-K forms a Monoid") = monoidLaws[(Map[Int, Int],Set[Int])]


  /**
   * Threshold crossing logic can also be done with one sum on
   * the following combined Semigroup:
   */
  property("Threshold combinator works") = {
    val threshold = 10000L

    implicit val orSemi = Semigroup.from[Boolean](_ || _)
    implicit val andSemi = Semigroup.from[Option[Boolean]] { (l, r) =>
      (l, r) match {
        case (None, _) => r
        case (_, None) => l
        case (Some(lb), Some(rb)) => Some(lb && rb)
      }
    }
    implicit val thresh: Semigroup[(Long, (Boolean, Option[Boolean]))] =
      new SemigroupCombinator({ (sum: Long, doneCross: (Boolean, Option[Boolean])) =>
        val (done, cross) = doneCross
        if(done) (true, Some(false))
        else {
          // just crossed
          if(sum >= threshold && doneCross._2.isEmpty) (true, Some(true))
          else (false, None) // not yet crossed
        }
      })
    semigroupLaws[(Long, (Boolean, Option[Boolean]))] && {
    // thresholds could be implemented as so:
    //
      def sumCrosses(l: List[Long], t: Long) =
        !(l.scanLeft(0L)(_ + _).filter( _ >= t).isEmpty)
      forAll { (t: List[Long]) =>
        sumCrosses(t, threshold) ==
          (Semigroup.sumOption((0L :: t).map { (_, (false, None:Option[Boolean])) })
             .headOption
             .map { _._2._1 }
             .getOrElse(false))
      }
    }
  }
}
