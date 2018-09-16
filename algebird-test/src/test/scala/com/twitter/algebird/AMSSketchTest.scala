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

import com.twitter.algebird.CMSInstance.CountsTable
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{Matchers, WordSpec}

object AMSTestUtils {

  def toInstances[A](ams: AMS[A]): AMSInstances[A] = ams match {
    case AMSZero(params) => AMSInstances(params)
    case AMSItem(it, count, params) =>
      (AMSInstances.apply(params) + it).asInstanceOf[AMSInstances[A]]
    case instance @ AMSInstances(_, _, _) => instance
  }
}

class AMSSketchMonoidTest extends CheckProperties {

  import BaseProperties._

  val depht = 2
  val buckets = 2

  implicit val amsMonoid: AMSMonoid[String] =
    new AMSMonoid[String](depht, buckets)

  implicit val amsGen = Arbitrary {
    val item = Gen.choose(1, 1000).map { v =>
      amsMonoid.create(v.toString)
    }
    val dense = Gen.listOf(item).map { it =>
      AMSTestUtils.toInstances[String](amsMonoid.sum(it))
    }
    val zero = Gen.const(amsMonoid.zero)
    Gen.frequency((4, item), (1, zero), (1, dense))
  }

  implicit def amsEquiv[K]: Equiv[AMS[K]] =
    new Equiv[AMS[K]] {
      def equiv(x: AMS[K], y: AMS[K]): Boolean = {
        val r = x == y
        r
      }
    }
  property("AMSSKetch is a monoid ") {
    commutativeMonoidLaws[AMS[String]]
  }

  property("++ is the same as plus") {
    forAll { (a: AMS[String], b: AMS[String]) =>
      Equiv[AMS[String]].equiv(a ++ b, amsMonoid.plus(a, b))
    }
  }

}

class AMSMonoidSimpleProperties extends WordSpec with Matchers {
  val semi: AMSMonoid[String] = new AMSMonoid[String](2, 2)
  "an amsMonoid simple properties checker " should {
    "check simple associative equivalency mixing AMSItem and AMSInstance" in {

      val a =
        new AMSInstances[String](CountsTable(Vector(Vector(-8L, -2L), Vector(-3L, -7L))), semi.params, 10)
      val b = AMSItem[String]("907", 1, semi.params)
      val c = AMSItem[String]("868", 1, semi.params)

      assert(semi.plus(semi.plus(a, b), c) == semi.plus(a, semi.plus(b, c)))
    }

    "check simple sumpropertieswork for semigroups between two AMSItem " in {
      val head = AMSItem[String]("739", 1, semi.params)
      val tail = List[AMS[String]](AMSItem[String]("437", 1, semi.params))

      val sumOPT = semi.sumOption(head :: tail).get
      val plus = head ++ tail.head

      assert(sumOPT == plus)
    }

    "check simple sumpropertieswork for semigroups between AMSItem and AMSInstance " in {
      val head = AMSItem[String]("591", 1, semi.params)
      val tail = List[AMS[String]](
        new AMSInstances[String](CountsTable(Vector(Vector(-2, -3), Vector(-3, -2))), semi.params, 5))

      val sumOPT = semi.sumOption(head :: tail).get
      val plus = head ++ tail.head

      assert(sumOPT == plus)
    }

  }

}

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

    "return instance with exact count " in {
      val params = AMSParams[String](width, buckets)
      val amsIt = AMSItem[String]("item-0", 14, params)
      var res = amsIt + ("item-1", 1)
      println("count ==" + res.totalCount)
      res = res + ("item-1", 10)
      assert(res.totalCount == 25)
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
