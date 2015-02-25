/*
Copyright 2013 Twitter, Inc.

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

import org.scalatest._

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Arbitrary
import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Properties
import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Gen.choose

import java.util.Arrays

class QTreeLaws extends CheckProperties {
  import BaseProperties._

  implicit val qtSemigroup = new QTreeSemigroup[Long](6)
  implicit val qtGen = Arbitrary {
    for (
      v <- choose(0L, 10000L)
    ) yield (QTree(v))
  }

  property("QTree is associative") {
    isAssociative[QTree[Long]]
  }

}

class QTreeTest extends WordSpec with Matchers {
  def randomList(n: Long) = {
    (1L to n).map{ i => math.random }
  }

  def buildQTree(k: Int, list: Seq[Double]) = {
    val qtSemigroup = new QTreeSemigroup[Double](k)
    list.map{ QTree(_) }.reduce{ qtSemigroup.plus(_, _) }
  }

  def trueQuantile(list: Seq[Double], q: Double) = {
    val rank = math.floor(q * list.size).toInt
    val sorted = list.toList.sorted
    sorted(rank)
  }

  def trueRangeSum(list: Seq[Double], from: Double, to: Double) = {
    list.filter{ _ >= from }.filter{ _ < to }.sum
  }

  for (k <- (1 to 6))
    ("QTree with sizeHint 2^" + k) should {
      "always contain the true quantile within its bounds" in {
        val list = randomList(10000)
        val qt = buildQTree(k, list)
        val quantile = math.random
        val (lower, upper) = qt.quantileBounds(quantile)
        val truth = trueQuantile(list, quantile)
        assert(truth >= lower)
        assert(truth <= upper)
      }
      "always contain the true range sum within its bounds" in {
        val list = randomList(10000)
        val qt = buildQTree(k, list)
        val from = math.random
        val to = math.random
        val (lower, upper) = qt.rangeSumBounds(from, to)
        val truth = trueRangeSum(list, from, to)
        assert(truth >= lower)
        assert(truth <= upper)
      }
      "have size bounded by 2^(k+2)" in {
        val list = randomList(100000)
        val qt = buildQTree(k, list)
        assert(qt.size <= (1 << (k + 2)))
      }
    }
}
