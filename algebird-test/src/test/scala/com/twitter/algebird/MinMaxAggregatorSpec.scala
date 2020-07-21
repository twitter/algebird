package com.twitter.algebird

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MinMaxAggregatorSpec extends AnyWordSpec with Matchers {
  sealed trait TestElementParent
  case object TestElementA extends TestElementParent
  case object TestElementB extends TestElementParent
  case object TestElementC extends TestElementParent

  implicit val testOrdering: Ordering[TestElementParent] = Ordering.fromLessThan[TestElementParent]((x, y) =>
    (x, y) match {
      case (TestElementA, TestElementA) => false
      case (TestElementA, _)            => true
      case (TestElementB, TestElementB) => false
      case (TestElementB, TestElementA) => false
      case (TestElementB, TestElementC) => true
      case (TestElementC, _)            => false
    }
  )

  val data: List[TestElementParent] = List(TestElementC, TestElementA, TestElementB)

  "MinAggregator" should {
    "produce the minimum value" in {
      val agg = Min.aggregator[TestElementParent]
      assert(agg(data) == TestElementA)
    }
  }

  "MaxAggregator" should {
    "produce the maximum value" in {
      val agg = Max.aggregator[TestElementParent]
      assert(agg(data) == TestElementC)
    }
  }
}
