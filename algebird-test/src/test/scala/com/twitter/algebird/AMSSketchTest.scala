package com.twitter.algebird

import org.scalatest.{Matchers, WordSpec}

class AMSSketchFunction extends WordSpec with Matchers {

  " AMSFunction " should {
    "return random number " in {
      val randoms = AMSFunction.generateRandom(10)
      assert(randoms.size == 6)

    }

  }

}

class AMSSketchItemTest extends WordSpec with Matchers {

  val width = 10
  val buckets = 15

  "an AMSItem " should {
    "return an instance with other item" in {
      val params = AMSParams[String](width, buckets)
      val amsIt = AMSItem[String]("item-0", 1, params)
      val res = amsIt + ("item-1", 1)
      assert(res.totalCount == 2)
      assert(res.isInstanceOf[AMSInstances[String]])
    }
  }
}

class AMSSketchInstanceTest extends WordSpec with Matchers {
  val width = 10
  val buckets = 15
  "AMSSketch instance " should {

    "add item and update the count " in {
      val params = AMSParams[String](width, buckets)
      val aMSInstances = AMSInstances(params)
      val res = aMSInstances + ("item-2", 1)
      assert(res.totalCount == 1)
    }

  }

}
