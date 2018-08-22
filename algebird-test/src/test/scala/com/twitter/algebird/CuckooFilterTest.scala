package com.twitter.algebird
import org.scalatest.{Matchers, WordSpec}

class CuckooFilterTest extends WordSpec with Matchers {


  "a cuckoo filter " in {

    "have to choose the first bucket he need " in {
    }
  }

  "a cuckoo hash" in {

    "produce 1 hash value" in {
      val cuckooHash = CFHash[String]()
      val hashed = cuckooHash("elmt")
      assert(hashed.length == 2)
    }



  }


}
