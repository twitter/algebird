package com.twitter.algebird

import org.scalatest.{Matchers, WordSpec}

class AMSSketchFunction extends WordSpec with Matchers{

  "An AMS Function" should {
    "return proper number of hashes : " in {
      val hashes = AMSFunction.generateHash[Int](10, 10)
      assert(hashes.size == 10)
    }
  }
}


class AMSSketchTest extends WordSpec with Matchers{

  "An AMS Sketch" should {



  }
}
