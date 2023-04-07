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

import java.util.PriorityQueue

import com.twitter.algebird.mutable.PriorityQueueMonoid
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

import scala.jdk.CollectionConverters._

class TopKTests extends CheckProperties {
  import com.twitter.algebird.BaseProperties._
  val SIZE: Int = 10

  implicit def qmonoid: PriorityQueueMonoid[Int] = new PriorityQueueMonoid[Int](SIZE)
  implicit def queueArb: Arbitrary[PriorityQueue[Int]] = Arbitrary {
    implicitly[Arbitrary[List[Int]]].arbitrary.map(qmonoid.build(_))
  }

  def q2l(q: PriorityQueue[Int]): List[Int] = q.iterator.asScala.toList.sorted

  implicit val eq: Equiv[PriorityQueue[Int]] = Equiv.by(q2l)

  def pqIsCorrect(items: List[List[Int]]): Boolean = {
    val correct = items.flatten.sorted.take(SIZE)
    // Have to do this last since this monoid is mutating inputs
    q2l(Monoid.sum(items.map(l => qmonoid.build(l)))) == correct
  }

  property("PriorityQueueMonoid works") {
    forAll((items: List[List[Int]]) => pqIsCorrect(items))
  }

  /**
   * The following were specific bugs that we failed some prior scalacheck (yay for randomized testing)
   */
  val pqPriorBugs: Seq[List[List[Int]]] = Seq(List(List(1, 1, 1, 2), List(0, 0, 0, 0, 0, 0, 0)))
  property("Specific regressions are handled") {
    pqPriorBugs.forall(pqIsCorrect(_))
  }

  property("PriorityQueueMonoid is a Monoid") {
    monoidLaws[PriorityQueue[Int]]
  }

  implicit def tkmonoid: TopKMonoid[Int] = new TopKMonoid[Int](SIZE)

  implicit def topkArb: Arbitrary[TopK[Int]] = Arbitrary {
    implicitly[Arbitrary[List[Int]]].arbitrary.map(tkmonoid.build(_))
  }

  property("TopKMonoid works") {
    forAll { (its: List[List[Int]]) =>
      val correct = its.flatten.sorted.take(SIZE)
      Equiv[List[Int]].equiv(
        Monoid
          .sum(its.map(l => tkmonoid.build(l)))
          .items,
        correct
      )
    }
  }

  property("TopKMonoid is a Monoid") {
    monoidLaws[PriorityQueue[Int]]
  }

}
