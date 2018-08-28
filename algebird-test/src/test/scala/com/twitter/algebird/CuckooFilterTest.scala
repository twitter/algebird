
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


class CuckooFilterLawsTest extends CheckProperties {

  val NB_BUCKET = 32
  val FP_BUCKET = 8

  implicit val cfMonoid: CuckooFilterMonoid[String] = new CuckooFilterMonoid[String](FP_BUCKET, NB_BUCKET)

  implicit val cfGen: Arbitrary[CF[String]] = Arbitrary {
    val item = Gen.choose(0, 10000).map { v =>
      cfMonoid.create(v.toString)
    }

    val zero = Gen.const(cfMonoid.zero)
    Gen.frequency((5, item))
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
      val a = new CFItem[String]("Aline", new CFHash[String](255), 5)
      val b = new CFItem[String]("pour qu'elle revienne", new CFHash[String](255), 5)
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
      assert((cuckooTest.cuckooBitSet.map(_.cardinality()) foldLeft 0) (_ + _) == 3)
    }

  }

  //
}
