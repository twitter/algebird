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

package com.twitter.algebird.maps.nearest

import org.scalatest._

import com.twitter.algebird.matchers.seq._

object NearestSetProperties extends FlatSpec with Matchers {
  import tree._
  import infra._

  def testNearest[IN <: INodeNear[Double], M <: NearestSetLike[Double, IN, M]](
    data: Seq[Double],
    map: NearestSetLike[Double, IN, M]) {
    val n = data.length
    // I'm writing this test assuming Double keys (0.0, 1.0, 2.0, ... (n-1))
    data should beEqSeq(Vector.tabulate(n)(j => j.toDouble))
    map.keys should beEqSeq(Vector.tabulate(n)(j => j.toDouble))

    map.nearest(-1e100) should beEqSeq(Seq(0.0))
    map.nearest(+1e100) should beEqSeq(Seq((n - 1).toDouble))

    (0.0 to (n - 2).toDouble by 1.0).foreach { j =>
      map.nearest(j) should beEqSeq(Seq(j))
      map.nearest(j + 0.4) should beEqSeq(Seq(j))
      map.nearest(j + 0.5) should beEqSeq(Seq(j, j + 1.0))
      map.nearest(j + 0.6) should beEqSeq(Seq(j + 1.0))
      map.nearest(j + 1.0) should beEqSeq(Seq(j + 1.0))
    }
  }
}

object NearestMapProperties extends FlatSpec with Matchers {
  import tree._
  import infra._

  def testNearest[V, IN <: INodeNearMap[Double, V], M <: NearestMapLike[Double, V, IN, M]](
    data: Seq[(Double, V)],
    map: NearestMapLike[Double, V, IN, M]) {
    val n = data.length
    // I'm writing this test assuming Double keys (0.0, 1.0, 2.0, ... (n-1))
    data.map(_._1) should beEqSeq(Vector.tabulate(n)(j => j.toDouble))
    map.keys should beEqSeq(Vector.tabulate(n)(j => j.toDouble))

    map.nearest(-1e100) should beEqSeq(Seq(((0.0, map.get(0.0).get))))
    map.nearest(+1e100) should beEqSeq(Seq((((n - 1).toDouble, map.get((n - 1).toDouble).get))))

    (0.0 to (n - 2).toDouble by 1.0).foreach { j =>
      val (vj, vj1) = (map.get(j).get, map.get(j + 1.0).get)
      map.nearest(j) should beEqSeq(Seq(((j, vj))))
      map.nearest(j + 0.4) should beEqSeq(Seq(((j, vj))))
      map.nearest(j + 0.5) should beEqSeq(Seq(((j, vj)), ((j + 1.0, vj1))))
      map.nearest(j + 0.6) should beEqSeq(Seq(((j + 1.0, vj1))))
      map.nearest(j + 1.0) should beEqSeq(Seq(((j + 1.0, vj1))))
    }
  }
}

class NearestSetSpec extends FlatSpec with Matchers {
  import com.twitter.algebird.maps.ordered.RBProperties._
  import com.twitter.algebird.maps.ordered.OrderedSetProperties._

  import NearestSetProperties._

  def mapType1 = NearestSet.key[Double]

  it should "pass randomized tree patterns" in {

    // empty map returns empty seq
    mapType1.nearest(1.0) should beEqSeq(Seq.empty[Double])

    val data = Vector.tabulate(50)(j => j.toDouble)
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val map = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(map)
      testK(data, map)
      testDel(data, map)
      testEq(data, mapType1)
      testNearest(data, map)
    }
  }

  it should "serialize and deserialize" in {
    import com.twitter.algebird.SerDe.roundTripSerDe

    val data = Vector.tabulate(50)(j => j.toDouble)
    val omap = data.foldLeft(mapType1)((m, e) => m + e)
    val imap = roundTripSerDe(omap)

    (imap == omap) should be (true)
    testRB(imap)
    testK(data, imap)
    testDel(data, imap)
    testNearest(data, imap)
  }
}

class NearestMapSpec extends FlatSpec with Matchers {
  import com.twitter.algebird.maps.ordered.RBProperties._
  import com.twitter.algebird.maps.ordered.OrderedMapProperties._

  import NearestMapProperties._

  def mapType1 = NearestMap.key[Double].value[Int]

  it should "pass randomized tree patterns" in {

    // empty map returns empty seq
    mapType1.nearest(1.0) should beEqSeq(Seq.empty[(Double, Int)])

    val data = Vector.tabulate(50)(j => (j.toDouble, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val map = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(map)
      testKV(data, map)
      testDel(data, map)
      testEq(data, mapType1)
      testNearest(data, map)
    }
  }

  it should "serialize and deserialize" in {
    import com.twitter.algebird.SerDe.roundTripSerDe

    val data = Vector.tabulate(50)(j => (j.toDouble, j))
    val omap = data.foldLeft(mapType1)((m, e) => m + e)
    val imap = roundTripSerDe(omap)

    (imap == omap) should be (true)
    testRB(imap)
    testKV(data, imap)
    testDel(data, imap)
    testNearest(data, imap)
  }
}
