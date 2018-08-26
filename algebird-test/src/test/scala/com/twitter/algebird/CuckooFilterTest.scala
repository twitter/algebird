
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

import com.googlecode.javaewah.datastructure.BitSet
import org.scalatest.{Matchers, WordSpec}

//class CuckooFilterLaws extends CheckProperties {
//
//  property("CuckooFilter is a Monoid") {
//    commutativeMonoidLaws[CF[String]]
//  }
//
//}


class CuckooFilterTest extends WordSpec with Matchers {

    "a cuckoo filter " should {

      "Add a fingerprint to the filter" in {
        val bs = Array.fill[BitSet](255)(new BitSet(8 * 8 + 8))

        val cuckooTest = CFInstance[String](new CFHash[String](), bs, 8, 255)
        val hashedValue = 114
        val cuckoo = cuckooTest + "item-1"
        assert(cuckoo.cuckooBitSet(hashedValue).cardinality() == 1)
      }

      "Add to other bucket if first bucket is full" in {
        val bs = Array.fill[BitSet](255)(new BitSet(8 * 8 + 8))

        var cuckooTest = CFInstance[String](new CFHash[String](), bs, 1, 255)
        val secondHasValue = 17
        cuckooTest = cuckooTest + "item-1"
        cuckooTest = cuckooTest + "item-1"
        assert(cuckooTest.cuckooBitSet(secondHasValue).cardinality() == 1)
      }

      "should kick a element if buckets are full" in {
        val bs = Array.fill[BitSet](255)(new BitSet(8 * 8 + 8))

        var cuckooTest = CFInstance[String](new CFHash[String](), bs, 1, 255)
        cuckooTest = cuckooTest + "item-10"
        cuckooTest = cuckooTest + "item-10"
        cuckooTest = cuckooTest + "item-10"
        assert((cuckooTest.cuckooBitSet.map(_.cardinality()) foldLeft 0) (_ + _) == 3)
      }

    }


}
