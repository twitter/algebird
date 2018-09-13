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

class AMSSketchItemTest extends WordSpec with Matchers {

  val width = 10
  val buckets = 15

  "an AMSItem " should {
    "return an instance with other item" in {
      val params = AMSParams[String](AMSFunction.generateHash[String](width, buckets), width, buckets)
      val amsIt = AMSItem[String]("item-0", 1,  params)
      val res = amsIt + ("item-1", 1)

      assert(res.totalCount == 2)
      assert(res.isInstanceOf[AMSInstances[String]])
    }

  }
}
