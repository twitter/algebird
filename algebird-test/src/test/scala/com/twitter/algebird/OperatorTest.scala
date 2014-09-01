package com.twitter.algebird

import org.scalatest._

import Operators._

class OperatorTest extends WordSpec with Matchers with DiagrammedAssertions {

  "Operators" should {
    "allow plus" in {
      assert(List(1, 2) + List(3, 4) == List(1, 2, 3, 4))
    }
    "allow -" in {
      assert((1, 3) - (2, 4) == (-1, -1))
    }
    "allow *" in {
      assert(Map(1 -> 3) * Map(2 -> 4) == Map[Int, Int]())
      assert(Map(1 -> 3) * Map(1 -> 4) == Map(1 -> 12))
    }
    "allow /" in {
      assert(true / true == true)
    }
  }
}
