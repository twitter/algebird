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

package com.twitter.algebird.maps.prefixsum

import org.scalatest._

import com.twitter.algebird.Monoid

import com.twitter.algebird.matchers.seq._

object PrefixSumMapProperties extends FlatSpec with Matchers {
  import tree._
  import infra._

  // Assumes 'data' is in key order
  def testPrefix[K, V, P, IN <: INodePS[K, V, P], M <: PrefixSumMapLike[K, V, P, IN, M] with Map[K, V]](
    data: Seq[(K, V)],
    psmap: PrefixSumMapLike[K, V, P, IN, M] with Map[K, V]) {

    val mon = psmap.prefixMonoid
    val psTruth = data.map(_._2).scanLeft(mon.zero)((v, e) => mon.inc(v, e))
    psmap.prefixSums() should beEqSeq(psTruth.tail)
    psmap.prefixSums(open = true) should beEqSeq(psTruth.dropRight(1))
    psmap.prefixSums() should beEqSeq(psmap.keys.map(k => psmap.prefixSum(k)))
    psmap.prefixSums(open = true) should beEqSeq(psmap.keys.map(k => psmap.prefixSum(k, open = true)))
  }
}

class PrefixSumMapSpec extends FlatSpec with Matchers {
  import com.twitter.algebird.maps.ordered.RBProperties._
  import com.twitter.algebird.maps.ordered.OrderedMapProperties._

  import PrefixSumMapProperties._

  def mapType1 =
    PrefixSumMap.key[Int].value[Int]
      .prefix(IncrementingMonoid.fromMonoid(implicitly[Monoid[Int]]))

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => (j, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val psmap = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(psmap)
      testKV(data, psmap)
      testDel(data, psmap)
      testEq(data, mapType1)
      testPrefix(data, psmap)
    }
  }

  it should "serialize and deserialize" in {
    import com.twitter.algebird.SerDe.roundTripSerDe

    val data = Vector.tabulate(50)(j => (j, j))
    val omap = data.foldLeft(mapType1)((m, e) => m + e)
    val imap = roundTripSerDe(omap)

    (imap == omap) should be (true)
    testRB(imap)
    testKV(data, imap)
    testDel(data, imap)
    testPrefix(data, imap)
  }
}
