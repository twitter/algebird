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
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose

object SummingIteratorTest extends Properties("SummingIterator") {

  def groupBySum[K,V:Semigroup](l: List[(K,V)]): Map[K,V] =
    l.groupBy { _._1 }.mapValues { g => Semigroup.sumOption(g.map { _._2 }).get }

  case class Capacity(c: Int)
  implicit val capArb = Arbitrary { for(c <- choose(0, 10240)) yield Capacity(c) }

  property("groupBySum is preserved[(Short,Int)], all in memory") = forAll { (items: List[(Short, Int)]) =>
      val distinctKeys = items.groupBy { _._1 }.size
      val sumit = SummingIterator(distinctKeys, items.iterator)
      val summedList = sumit.toList
      (groupBySum(summedList) == groupBySum(items)) && (summedList.size == distinctKeys)
  }
  property("groupBySum is preserved[Int]") = forAll { (cap: Capacity, items: List[(Short, Int)]) =>
    val sumit = SummingIterator(cap.c, items.iterator)
    val summedList = sumit.toList
    (groupBySum(summedList) == groupBySum(items)) &&
      (summedList.size <= items.size)
  }
  property("groupBySum is preserved[String]") = forAll { (cap: Capacity, items: List[(Short, String)]) =>
    val sumit = SummingIterator(cap.c, items.iterator)
    val summedList = sumit.toList
    (groupBySum(summedList) == groupBySum(items)) &&
      (summedList.size <= items.size)
  }
}
