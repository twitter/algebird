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

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import scala.util.Random

object SummingCacheTest {
  case class Capacity(cap: Int) extends AnyVal
  implicit val capArb: Arbitrary[Capacity] = Arbitrary {
    for (c <- Gen.choose(0, 1024)) yield Capacity(c)
  }
}

class SummingCacheTest extends CheckProperties {
  import SummingCacheTest._

  // Get the zero-aware map equiv
  import SummingIteratorTest.mapEquiv

  def newCache[K, V: Monoid](c: Capacity): StatefulSummer[Map[K, V]] =
    SummingCache[K, V](c.cap)

  // Maps are tricky to compare equality for since zero values are often removed
  def test[K, V: Monoid](c: Capacity, items: List[(K, V)]): Boolean = {
    val sc = newCache[K, V](c)
    val mitems = items.map(Map(_))
    implicit val mapEq = mapEquiv[K, V]
    StatefulSummerLaws.sumIsPreserved(sc, mitems) &&
    StatefulSummerLaws.isFlushedIsConsistent(sc, mitems)
  }

  property("puts are like sums (Int, Int)") {
    forAll((c: Capacity, items: List[(Int, Int)]) => test(c, items))
  }
  // String is not commutative:
  property("puts are like sums (Int, List[Int])") {
    forAll((c: Capacity, items: List[(Int, List[Int])]) => test(c, items))
  }
}

class AdaptiveCacheTest extends SummingCacheTest {
  import SummingCacheTest._

  override def newCache[K, V: Monoid](c: Capacity): StatefulSummer[Map[K, V]] =
    new AdaptiveCache[K, V](c.cap)
}

class SummingWithHitsCacheTest extends SummingCacheTest {
  import SummingCacheTest._

  val RAND: Random = new scala.util.Random

  def getHits[K, V: Monoid](c: Capacity, items: List[(K, V)]): List[Int] = {
    val sc = SummingWithHitsCache[K, V](c.cap)
    val mitems = items.map(Map(_))
    mitems.map(sc.putWithHits(_)._1).tail
  }

  property("hit rates will always be 1 for stream with the same key") {
    forAll { (c: Capacity, values: List[Int]) =>
      // Only run this when we have at least 2 items and non-zero cap
      (values.size > 1 && c.cap > 1) ==> {
        val key = RAND.nextInt
        val items = values.map((key, _))
        val keyHits = getHits(c, items)
        !keyHits.exists(_ != 1)
      }
    }
  }

  property("hit rates will always be 0 when cap is 0") {
    forAll { items: List[(Int, Int)] =>
      // Only run this when we have at least 2 items
      (items.size > 1) ==> {
        val keyHits = getHits(Capacity(0), items)
        !keyHits.exists(_ != 0)
      }
    }
  }

  property("hit rates in general should be between [0, 1] ") {
    forAll { (c: Capacity, items: List[(Int, Int)]) =>
      // Only run this when we have at least 2 items
      (items.size > 1) ==> {
        val keyHits = getHits(c, items)
        val hitRate = keyHits.sum / items.size.toDouble
        hitRate >= 0 && hitRate <= 1
      }
    }
  }
}

class SummingQueueTest extends CheckProperties {
  val zeroCapQueue: SummingQueue[Int] = SummingQueue[Int](0) // passes all through

  property("0 capacity always returns") {
    forAll { i: Int => zeroCapQueue(i) == Some(i) }
  }

  val sb: SummingQueue[Int] = SummingQueue[Int](3) // buffers three at a time

  property("puts are like sums") {
    forAll((items: List[Int]) => StatefulSummerLaws.sumIsPreserved(sb, items))
  }

  property("puts are like sums(String)") {
    forAll { (items: List[String]) =>
      val sbs = SummingQueue[String](3) // buffers three at a time
      StatefulSummerLaws.sumIsPreserved(sbs, items)
    }
  }

  property("isFlushed is consistent") {
    forAll((items: List[Int]) => StatefulSummerLaws.isFlushedIsConsistent(sb, items))
  }

  property("puts return None sometimes") {
    forAll { (items: List[Int]) =>
      // Should be: true, true, true, false, true, true, true, false
      sb.flush
      val empties = items.map(sb.put(_).isEmpty)
      val correct = Stream
        .continually(Stream(true, true, true, false))
        .flatMap(s => s)
        .take(empties.size)
        .toList
      empties == correct
    }
  }
}
