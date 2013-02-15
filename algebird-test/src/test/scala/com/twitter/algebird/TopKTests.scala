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

import com.twitter.algebird.mutable.PriorityQueueMonoid
import java.util.PriorityQueue

import scala.collection.JavaConverters._

import org.scalacheck.{Arbitrary, Properties}
import org.scalacheck.Gen._
import org.specs.Specification
import org.scalacheck.Prop.forAll

object TopKTests extends Properties("TopKTests") {
  import BaseProperties._
  val SIZE = 10

  property("SortedTakeListMonoid is a Monoid") = {
    implicit val listMonoid = new SortedTakeListMonoid[Int](SIZE)
    implicit val queueArb = Arbitrary {
      // This monoid assumes it is getting sorted lists to operate on
      implicitly[Arbitrary[List[Int]]].arbitrary.map { _.sorted.take(SIZE) }
    }
    monoidLaws[List[Int]]
  }
  property("SortedTakeListMonoid works") = {
    implicit val listMonoid = new SortedTakeListMonoid[Int](SIZE)
    implicit val queueArb = Arbitrary {
      // This monoid assumes it is getting sorted lists to operate on
      implicitly[Arbitrary[List[Int]]].arbitrary.map { _.sorted.take(SIZE) }
    }
    forAll { (items: List[List[Int]]) =>
      val byHand = (items.flatten.sorted.take(SIZE))
      Monoid.sum(items) == byHand
    }
  }
  implicit def qmonoid = new PriorityQueueMonoid[Int](SIZE)
  implicit def queueArb = Arbitrary {
    implicitly[Arbitrary[List[Int]]].arbitrary.map { qmonoid.build(_) }
  }
  def q2l(q1: PriorityQueue[Int]): List[Int] = q1.iterator.asScala.toList.sorted

  def eqFn(q1: PriorityQueue[Int], q2: PriorityQueue[Int]): Boolean = {
    q2l(q1) == q2l(q2)
  }
  property("PriorityQueueMonoid works") = forAll { (items: List[List[Int]]) =>
    val correct = items.flatten.sorted.take(SIZE)
    // Have to do this last since this monoid is mutating inputs
    q2l(Monoid.sum(items.map { l => qmonoid.build(l) } )) == correct
  }
  property("PriorityQueueMonoid is a Monoid") = monoidLawsEq[PriorityQueue[Int]](eqFn)
/*
  import scala.collection.SortedSet
  implicit def smonoid: Monoid[SortedSet[Int]] = new SortedSetMonoid[Int](SIZE)
  implicit def setArb = Arbitrary {
    implicitly[Arbitrary[List[Int]]].arbitrary.map { l => SortedSet(l : _*) }
  }
  property("SortedSetMonoid works") = forAll { (items: List[SortedSet[Int]]) =>
    val correct = items.map { _.toList }.flatten.sorted.take(SIZE)
    // Have to do this last since this monoid is mutating inputs
    Monoid.sum(items) == SortedSet(correct : _*)
  }
  property("SortedSetMonoid is a Monoid") = monoidLaws[SortedSet[Int]]
  */
}
