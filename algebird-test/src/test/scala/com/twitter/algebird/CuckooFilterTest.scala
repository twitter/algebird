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

import com.googlecode.javaewah.{EWAHCompressedBitmap => CBitSet}
import com.twitter.algebird.BaseProperties.commutativeMonoidLaws
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{Matchers, WordSpec}

object CuckooTestUtils {

  def toInstances[A](cf: CF[A]): CFInstance[A] = cf match {
    case CFZero(hash, fp, bck)             => CFInstance[A](hash, fp, bck)
    case CFItem(hash, fp, bck, it)         => CFInstance.apply[A](hash, fp, bck) + it
    case instance @ CFInstance(_, _, _, _) => instance
  }
}

class CuckooFilterLawsTest extends CheckProperties {

  import CuckooTestUtils._

  val NB_BUCKET = 256
  val FP_BUCKET = 64

  implicit val cfMonoid: CuckooFilterMonoid[String] = new CuckooFilterMonoid[String](FP_BUCKET, NB_BUCKET)

  implicit val cfGen: Arbitrary[CF[String]] = Arbitrary {
    val item = Gen.choose(0, 10000).map { v =>
      cfMonoid.create(v.toString)
    }
    val dense = Gen.listOf(item).map { its =>
      toInstances[String](cfMonoid.sum(its))
    }
    val zero = Gen.const(cfMonoid.zero)
    Gen.frequency((10, dense), (5, item))
  }

  property("CuckooFilter  is a Monoid") {
    commutativeMonoidLaws[CF[String]]
  }

  property("++ is the same as plus") {
    forAll { (a: CF[String], b: CF[String]) =>
      Equiv[CF[String]].equiv(a ++ b, cfMonoid.plus(a, b))
    }
  }

  property("+ is the same as adding with create") {
    forAll { (a: CF[String], b: String) =>
      Equiv[CF[String]].equiv(a + b, cfMonoid.plus(a, cfMonoid.create(b)))
    }
  }
  property("a ++ a = a for CF") {
    forAll { (a: CF[String]) =>
      Equiv[CF[String]].equiv(a ++ a, a)
    }
  }

  property("after + lookup is true") {
    forAll { (a: CF[String], b: String) =>
      (a + b).lookup(b)
    }
  }

}

class CuckooFilterHashTest extends CheckProperties {
  val NUM_HASHES = 10000
  val BCK_SIZE = 300

  implicit val cfHash: Arbitrary[CFHash[String]] = Arbitrary {
    for {
      bckSize <- Gen.choose(100, BCK_SIZE)
    } yield CFHash[String](bckSize)
  }

  property("Hash is always positiv ") {
    forAll { (hash: CFHash[String], v: Long) =>
      val (h, k, fp) = hash.apply(v.toString)
      h >= 0 && k >= 0 && fp >= 0
    }
  }

}

class CuckooFilterTest extends WordSpec with Matchers {

  val RAND = new scala.util.Random

  "a cuckoo item " should {
    "be ++ with other CFItem" in {
      val a = new CFItem[String](new CFHash[String](255), 5, 255, "Aline")
      val b = new CFItem[String](new CFHash[String](255), 5, 255, "pour qu'elle revienne")
      val c = a ++ b
      assert(c.isInstanceOf[CFInstance[String]])
      assert(c.lookup("Aline") && c.lookup("pour qu'elle revienne"))
    }

  }

  "a cuckoo filter " should {

    // 255 buckets by default
    "be constructed from an iterator" in {
      val cfMonoid = new CuckooFilterMonoid[String](RAND.nextInt(10) + 1)

      val entries = (0 until 10).map(_ => RAND.nextInt.toString)
      val monoid = cfMonoid.create(entries.iterator)
      assert(monoid.isInstanceOf[CF[String]])
    }

    // 255 buckets by default
    "be constructed from an seq" in {
      val cfMonoid = new CuckooFilterMonoid[String](RAND.nextInt(10) + 1)

      val entries = (0 until 10).map(_ => RAND.nextInt.toString)
      val monoid = cfMonoid.create(entries: _*)
      assert(monoid.isInstanceOf[CF[String]])
    }

    "Add a fingerprint to the filter" in {
      val bs = Array.fill[CBitSet](255)(new CBitSet(64 * 4))

      val cuckooTest = CFInstance[String](new CFHash[String](255), bs, 1, 255)
      val hashedValue = 75
      var cuckoo = cuckooTest + "item-1"

      assert(cuckoo.cuckooBitSet(hashedValue).cardinality() == 1)

    }
    "Add to other bucket if first bucket is full" in {
      val bs = Array.fill[CBitSet](255)(new CBitSet(8 * 8 + 8))

      var cuckooTest = CFInstance[String](new CFHash[String](255), bs, 1, 255)
      val secondHasValue = 184
      cuckooTest = cuckooTest + "item-1"
      cuckooTest = cuckooTest + "item-1"
      assert(cuckooTest.cuckooBitSet(secondHasValue).cardinality() == 1)
    }

    "Kick a element if buckets are full" in {
      val bs = Array.fill[CBitSet](255)(new CBitSet(8 * 8 + 8))

      var cuckooTest = CFInstance[String](new CFHash[String](255), bs, 1, 255)
      cuckooTest = cuckooTest + "item-10"
      cuckooTest = cuckooTest + "item-10"
      cuckooTest = cuckooTest + "item-10"
      assert(cuckooTest.cuckooBitSet.map(_.cardinality()).sum == 3)
    }

    "work as an Aggregator" in {
      (0 to 10).foreach { _ =>
        {
          val aggregator = CuckooFilterAggregator[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
          val numEntries = 5
          val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
          val bf = aggregator(entries)

          entries.foreach { i =>
            assert(bf.lookup(i))
          }
        }
      }
    }

    "be used like a bloomfilter" in {
      val bfMonoid1 = new CuckooFilterMonoid[String](32, 256)
      val bf1 = bfMonoid1.create("1", "2", "3", "4", "100")
      val lookup = bf1.lookup("1")
      assert(lookup)

    }
  }
}
