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

object SummingIteratorTest {
  implicit def mapEquiv[K, V: HasAdditionOperatorAndZero: Equiv]: Equiv[Map[K, V]] = Equiv.fromFunction { (l, r) =>
    val zl = MapAlgebra.removeZeros(l)
    val zr = MapAlgebra.removeZeros(r)
    zl.size == zr.size && {
      zl.forall {
        case (k, v) =>
          zr.get(k).map { rv => Equiv[V].equiv(rv, v) }.getOrElse(false)
      }
    }
  }
}

class SummingIteratorTest extends PropSpec with PropertyChecks with Matchers {
  import SummingIteratorTest._
  def sumEquiv[V: HasAdditionOperator: Equiv](it0: Iterator[V], it1: Iterator[V]): Boolean =
    StatefulSummerLaws.zeroEquiv(HasAdditionOperator.sumOption(it0), HasAdditionOperator.sumOption(it1))

  case class Capacity(c: Int)
  implicit val capArb = Arbitrary { for (c <- Gen.choose(0, 10240)) yield Capacity(c) }

  property("With Maps is preserved[(Short,Int)]") {
    forAll { (cap: Capacity, items: List[(Short, Int)]) =>
      val mitems = items.map { Map(_) }
      val qit = SummingIterator[Map[Short, Int]](SummingQueue[Map[Short, Int]](cap.c), mitems.iterator)
      val qitc = SummingIterator[Map[Short, Int]](SummingCache[Short, Int](cap.c), mitems.iterator)
      assert(sumEquiv(mitems.iterator, qit) && sumEquiv(mitems.iterator, qitc))
    }
  }

  property("With Maps is preserved[(Short,String)]") {
    forAll { (cap: Capacity, items: List[(Short, String)]) =>
      val mitems = items.map { Map(_) }
      val qit = SummingIterator(SummingQueue[Map[Short, String]](cap.c), mitems.iterator)
      val qitc = SummingIterator(SummingCache[Short, String](cap.c), mitems.iterator)
      assert(sumEquiv(mitems.iterator, qit) && sumEquiv(mitems.iterator, qitc))
    }
  }
}
