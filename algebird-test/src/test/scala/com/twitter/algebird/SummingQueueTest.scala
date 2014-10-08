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

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

object SummingCacheTest {
  case class Capacity(cap: Int) extends AnyVal
  implicit val capArb = Arbitrary { for (c <- Gen.choose(0, 1024)) yield Capacity(c) }
}

class SummingCacheTest extends PropSpec with PropertyChecks with Matchers {
  import SummingCacheTest._

  // Get the zero-aware map equiv
  import SummingIteratorTest.mapEquiv

  // Maps are tricky to compare equality for since zero values are often removed
  def test[K, V: Monoid](c: Capacity, items: List[(K, V)]) {
    val sc = SummingCache[K, V](c.cap)
    val mitems = items.map { Map(_) }
    implicit val mapEq = mapEquiv[K, V]
    assert(StatefulSummerLaws.sumIsPreserved(sc, mitems) &&
      StatefulSummerLaws.isFlushedIsConsistent(sc, mitems))
  }

  property("puts are like sums (Int, Int)") {
    forAll { (c: Capacity, items: List[(Int, Int)]) =>
      test(c, items)
    }
  }
  // String is not commutative:
  property("puts are like sums (Int, List[Int])") {
    forAll { (c: Capacity, items: List[(Int, List[Int])]) =>
      test(c, items)
    }
  }
}

class SummingQueueTest extends PropSpec with PropertyChecks with Matchers {
  val zeroCapQueue = SummingQueue[Int](0) // passes all through

  property("0 capacity always returns") {
    forAll { i: Int =>
      assert(zeroCapQueue(i) == Some(i))
    }
  }

  val sb = SummingQueue[Int](3) // buffers three at a time

  property("puts are like sums") {
    forAll { (items: List[Int]) =>
      assert(StatefulSummerLaws.sumIsPreserved(sb, items))
    }
  }

  property("puts are like sums(String)") {
    forAll { (items: List[String]) =>
      val sbs = SummingQueue[String](3) // buffers three at a time
      assert(StatefulSummerLaws.sumIsPreserved(sbs, items))
    }
  }

  property("isFlushed is consistent") {
    forAll { (items: List[Int]) =>
      assert(StatefulSummerLaws.isFlushedIsConsistent(sb, items))
    }
  }

  property("puts return None sometimes") {
    forAll { (items: List[Int]) =>
      // Should be: true, true, true, false, true, true, true, false
      sb.flush
      val empties = items.map { sb.put(_).isEmpty }
      val correct = Stream
        .continually(Stream(true, true, true, false))
        .flatMap { s => s }
        .take(empties.size)
        .toList
      assert(empties == correct)
    }
  }
}
