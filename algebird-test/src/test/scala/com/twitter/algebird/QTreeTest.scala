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

import org.scalacheck.Arbitrary
import org.scalacheck.Gen.choose
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.immutable

class QTreeLaws extends CheckProperties {
  import BaseProperties._

  implicit val qtSemigroup: QTreeSemigroup[Long] = new QTreeSemigroup[Long](4)
  implicit val qtGen: Arbitrary[QTree[Long]] = Arbitrary {
    for (v <- choose(0L, 10000L)) yield (QTree(v))
  }

  property("QTree is associative") {
    isAssociative[QTree[Long]]
  }
}

class QTreeTest extends AnyWordSpec with Matchers {
  def randomList(n: Long): immutable.IndexedSeq[Double] =
    (1L to n).map(_ => math.random)

  def buildQTree(k: Int, list: Seq[Double]): QTree[Double] = {
    val qtSemigroup = new QTreeSemigroup[Double](k)
    qtSemigroup.sumOption(list.map(QTree(_))).get
  }

  def trueQuantile[T: Ordering](list: Seq[T], q: Double): T = {
    val rank = math.floor(q * list.size).toInt
    val sorted = list.toList.sorted
    sorted(rank)
  }

  def trueRangeSum(list: Seq[Double], from: Double, to: Double): Double =
    list.filter(_ >= from).filter(_ < to).sum

  for (k <- Seq(3, 11, 51, 101)) {
    s"QTree with elements (1 to $k)" should {
      val trueMedian = (1 + k) / 2
      s"have median $trueMedian" in {
        implicit val sg = new QTreeSemigroup[Unit](6)

        val list = (1 to k).map(_.toDouble)
        val qtree = sg.sumOption(list.map(QTree.value(_))).get

        val (lower, upper) = qtree.quantileBounds(0.5)
        assert(lower <= trueMedian && trueMedian <= upper)
      }
    }
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
      "return correct quantile bounds for two percentile extremes" in {
        val list = randomList(10000)
        val qt = buildQTree(k, list)
        val (lower, _) = qt.quantileBounds(0.0)
        val (_, upper) = qt.quantileBounds(1.0)
        assert(lower == 0.0)
        assert(upper == 1.0)
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
        val list = randomList(10000)
        val qt = buildQTree(k, list)
        assert(qt.size <= (1 << (k + 2)))
      }
    }

  for (quantile <- List(0, .05, .5, .777777777, .95))
    ("A QTreeAggregator with quantile set as " + quantile) should {
      "work as an aggregator for doubles with a small stream" in {
        val list = randomList(10000).map(i => math.round(i * 100).toDouble)
        val agg = QTreeAggregator(quantile)(implicitly[Numeric[Double]])
        val interval = agg(list)
        val truth = trueQuantile(list, quantile)
        assert(interval.contains(truth))
      }
      "work as an aggregator for longs with a small stream" in {
        val list = randomList(10000).map(i => (i * 1000L).toLong)
        val agg = QTreeAggregator(quantile)(implicitly[Numeric[Long]])
        val interval = agg(list)
        val truth = trueQuantile(list, quantile)
        assert(interval.contains(truth))
      }
    }
}
