package com.twitter.algebird
import org.scalatest.{Matchers, WordSpec}

class CuckooFilterTest extends WordSpec with Matchers {


  "a cuckoo filter " should {

    "have to choose the first bucket he need " in {
    }
  }

  "a cuckoo hash" should {

    "produce 1 hash value" in {
      val cuckooHash = CFHash[String]()
      val hashed = cuckooHash(1, "elmt")
      println("leading 0 in 5 ::" + Integer.numberOfLeadingZeros(16))
      println("leading 0 in 32 ::" + Integer.numberOfLeadingZeros(32))
      println("leading 0 in 64 ::" + Integer.numberOfLeadingZeros(64))
      println("leading 0 in 128::" + Integer.numberOfLeadingZeros(128))

      println("Value : " + hashed)
      println("hash % and the rest  " + (hashed & 0x7fffffffffffffffL))


      val buffer  = new Array[Byte]( 10 * 14)


      val f = 32 - Integer.numberOfLeadingZeros(5)
      println("leading 0 ::" + f)

    }



  }


}
