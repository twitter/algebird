package com.twitter.algebird

import org.specs2.mutable._

class MinMaxAggregatorTest extends Specification {
  val data = List(1, 3, 5, 0, 7, 6)

  sealed trait TestElementParent
  case object TestElementA extends TestElementParent
  case object TestElementB extends TestElementParent
  case object TestElementC extends TestElementParent

  implicit val testOrdering = Ordering.fromLessThan[TestElementParent]((x, y) => (x, y) match {
    case (TestElementA, TestElementA) => false
    case (TestElementA, _) => true
    case (TestElementB, TestElementB) => false
    case (TestElementB, TestElementA) => false
    case (TestElementB, TestElementC) => true
    case (TestElementC, _) => false
  })

  val data2 = List(TestElementC, TestElementA, TestElementB)

  "MinAggregator" should {
    "produce the minimum value" in {
      val agg = Min.aggregator[Int]
      agg(data) must be_==(0)

      val agg2 = Min.aggregator[TestElementParent]
      agg2(data2) must be_==(TestElementA)
    }
  }

  "MaxAggregator" should {
    "produce the maximum value" in {
      val agg = Max.aggregator[Int]
      agg(data) must be_==(7)

      val agg2 = Max.aggregator[TestElementParent]
      agg2(data2) must be_==(TestElementC)
    }
  }
}
