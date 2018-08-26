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



class CuckooFilterTest extends WordSpec with Matchers  {

  val mockHash =

  "a cuckoo filter " should {

    "Add a fingerprint to the filter" in {
      val bs = Array.fill[BitSet](255)(new BitSet(8 * 8 + 8))

      val cuckooTest = CFInstance[String](new CFHash[String](), bs, 8,  255)
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
      val secondHasValue = 17
      cuckooTest = cuckooTest + "item-10"
      cuckooTest = cuckooTest + "item-10"

      cuckooTest = cuckooTest + "item-1178189461"
    }

  }


}
