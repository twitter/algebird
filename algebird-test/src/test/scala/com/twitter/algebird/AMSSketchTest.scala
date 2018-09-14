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

import org.scalatest.{Matchers, WordSpec}

class AMSSketchMonoidTest



class AMSSketchFunction extends WordSpec with Matchers {

  " AMSFunction " should {
    "return random number " in {
      val randoms = AMSFunction.generateRandom(10)
      assert(randoms.size == 6)
      assert(randoms.forall(_.forall(_ >= 0)))
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

    "determine frequency for one item " in {
      val params = AMSParams[String](width, buckets)
      val aMSInstances = AMSInstances(params)
      val res = aMSInstances + ("item-2", 50)
      assert(res.frequency("item-2").estimate == 50)
    }

    "give a inner product between Two AMSinstances " in {
      val params = AMSParams[String](width, buckets)
      val aMSInstances1 = AMSInstances(params)
      val aMSInstances2 = AMSInstances(params)
      val res = aMSInstances1 + ("item-2", 50)
      val res1 = aMSInstances2 + ("item-2", 50)
      val inner = res.innerProduct(res1)

      assert(inner.estimate == 50 * 50)
    }

    "give a inner product of itself " in {
      val params = AMSParams[String](width, buckets)
      val aMSInstances1 = AMSInstances(params)
      val res = aMSInstances1 + ("item-2", 5)
      assert(res.innerProduct(res).estimate == 5 * 5)
    }

    "give a f2 moment of it " in {
      val params = AMSParams[String](width, buckets)
      val aMSInstances1 = AMSInstances(params)
      val res = aMSInstances1 + ("item-2", 5)
      val res1 = res + ("item-3", 67)
      assert(res1.f2.estimate == 67 * 67 + 25)
    }

    "give correct innerProduct with itself for several values " in {

      var ams: AMS[String] = null

      val data = Array(
        Array(0, 45),
        Array(3, 48),
        Array(6, 51),
        Array(9, 54),
        Array(12, 57),
        Array(15, 60),
        Array(18, 63),
        Array(21, 66),
        Array(24, 69),
        Array(27, 72),
        Array(30, 75),
        Array(33, 78),
        Array(36, 81),
        Array(39, 84),
        Array(42, 87)
      )

      val params = AMSParams[String](width, buckets)
      val aMSInstances1 = AMSInstances(params)

      data.foreach(d => {
        if (ams == null)
          ams = aMSInstances1 + (d(0).toString, d(1))
        else
          ams = ams + (d(0).toString, d(1))
      })

      assert(ams.innerProduct(ams).estimate > 0)
    }

  }

}
