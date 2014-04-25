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

object SummingCacheTest extends Properties("SummingCache") {
  case class Capacity(cap: Int)
  implicit val capArb = Arbitrary { for(c <- choose(0, 10240)) yield Capacity(c) }

  // Get the zero-aware map equiv
  import SummingIteratorTest.mapEquiv

  // Maps are tricky to compare equality for since zero values are often removed
  def test[K,V:Monoid](c: Capacity, items: List[(K,V)]) = {
    List(SummingCache[K, V](c.cap), PlusOptimizedSummingCache[K,V](c.cap), SumOptionOptimizedSummingCache[K,V](c.cap)).map((sc) => {
      val mitems = items.map { Map(_) }
      implicit val mapEq = mapEquiv[K,V]
      StatefulSummerLaws.sumIsPreserved(sc, mitems) &&
        StatefulSummerLaws.isFlushedIsConsistent(sc, mitems)
    }).reduce(_ && _)
  }

  property("puts are like sums (Int, Int)") = forAll { (c: Capacity, items: List[(Int,Int)]) =>
    test(c, items)
  }
  // String is not commutative:
  property("puts are like sums (Int, String)") = forAll { (c: Capacity, items: List[(Int,String)]) =>
    test(c, items)
  }
}

object SummingQueueTest extends Properties("SummingQueue") {
  val zeroCapQueue = SummingQueue[Int](0) // passes all through

  property("0 capacity always returns") = forAll { i: Int =>
    zeroCapQueue(i) == Some(i)
  }

  val sb = SummingQueue[Int](3) // buffers three at a time

  property("puts are like sums") = forAll { (items: List[Int]) =>
    StatefulSummerLaws.sumIsPreserved(sb, items)
  }
  property("puts are like sums(String)") = forAll { (items: List[String]) =>
    val sbs = SummingQueue[String](3) // buffers three at a time
    StatefulSummerLaws.sumIsPreserved(sbs, items)
  }
  property("isFlushed is consistent") = forAll { (items: List[Int]) =>
    StatefulSummerLaws.isFlushedIsConsistent(sb, items)
  }
  property("puts return None sometimes") = forAll { (items: List[Int]) =>
    // Should be: true, true, true, false, true, true, true, false
    sb.flush
    val empties = items.map { sb.put(_).isEmpty }
    val correct = Stream
      .continually( Stream(true, true, true, false) )
      .flatMap { s => s }
      .take(empties.size)
      .toList
    empties == correct
  }
}
