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

import com.twitter.algebird.mutable.PriorityQueueHasAdditionOperatorAndZero
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

import scala.collection.JavaConverters._

class TopKTests extends CheckProperties {
  import com.twitter.algebird.BaseProperties._
  val SIZE = 10

  implicit def qmonoid = new PriorityQueueHasAdditionOperatorAndZero[Int](SIZE)
  implicit def queueArb = Arbitrary {
    implicitly[Arbitrary[List[Int]]].arbitrary.map { qmonoid.build(_) }
  }

  def q2l(q1: PriorityQueue[Int]): List[Int] = q1.iterator.asScala.toList.sorted

  def eqFn(q1: PriorityQueue[Int], q2: PriorityQueue[Int]): Boolean = {
    q2l(q1) == q2l(q2)
  }

  def pqIsCorrect(items: List[List[Int]]): Boolean = {
    val correct = items.flatten.sorted.take(SIZE)
    // Have to do this last since this monoid is mutating inputs
    q2l(HasAdditionOperatorAndZero.sum(items.map { l => qmonoid.build(l) })) == correct
  }

  property("PriorityQueueHasAdditionOperatorAndZero works") {
    forAll { (items: List[List[Int]]) =>
      pqIsCorrect(items)
    }
  }
  /**
   * The following were specific bugs that we failed some prior
   * scalacheck (yay for randomized testing)
   */
  val pqPriorBugs = Seq(List(List(1, 1, 1, 2), List(0, 0, 0, 0, 0, 0, 0)))
  property("Specific regressions are handled") {
    pqPriorBugs.forall(pqIsCorrect(_))
  }

  property("PriorityQueueHasAdditionOperatorAndZero is a HasAdditionOperatorAndZero") {
    monoidLawsEq[PriorityQueue[Int]](eqFn)
  }

  implicit def tkmonoid = new TopKHasAdditionOperatorAndZero[Int](SIZE)

  implicit def topkArb = Arbitrary {
    implicitly[Arbitrary[List[Int]]].arbitrary.map { tkmonoid.build(_) }
  }

  property("TopKHasAdditionOperatorAndZero works") {
    forAll { (its: List[List[Int]]) =>
      val correct = its.flatten.sorted.take(SIZE)
      Equiv[List[Int]].equiv(HasAdditionOperatorAndZero.sum(its.map { l => tkmonoid.build(l) }).items, correct)
    }
  }

  property("TopKHasAdditionOperatorAndZero is a HasAdditionOperatorAndZero") {
    monoidLawsEq[PriorityQueue[Int]](eqFn)
  }

}
