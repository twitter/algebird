package com.twitter.algebird

import org.scalatest._

class TupleAggregatorsTest extends WordSpec with Matchers {
  // This gives you an implicit conversion from tuples of aggregators
  // to aggregator of tuples
  import GeneratedTupleAggregator._

  val data = List(1, 3, 2, 0, 5, 6)
  val MinAgg = Aggregator.min[Int]

  "GeneratedTupleAggregators" should {
    "Create an aggregator from a tuple of 2 aggregators" in {
      val agg: Aggregator[Int, Tuple2[Int, Int]] = Tuple2(MinAgg, MinAgg)
      assert(agg(data) == Tuple2(0, 0))
    }

    "Create an aggregator from a tuple of 3 aggregators" in {
      val agg: Aggregator[Int, Tuple3[Int, Int, Int]] = Tuple3(MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple3(0, 0, 0))
    }

    "Create an aggregator from a tuple of 4 aggregators" in {
      val agg: Aggregator[Int, Tuple4[Int, Int, Int, Int]] = Tuple4(MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple4(0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 5 aggregators" in {
      val agg: Aggregator[Int, Tuple5[Int, Int, Int, Int, Int]] = Tuple5(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple5(0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 6 aggregators" in {
      val agg: Aggregator[Int, Tuple6[Int, Int, Int, Int, Int, Int]] = Tuple6(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple6(0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 7 aggregators" in {
      val agg: Aggregator[Int, Tuple7[Int, Int, Int, Int, Int, Int, Int]] = Tuple7(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple7(0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 8 aggregators" in {
      val agg: Aggregator[Int, Tuple8[Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple8(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple8(0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 9 aggregators" in {
      val agg: Aggregator[Int, Tuple9[Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple9(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple9(0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 10 aggregators" in {
      val agg: Aggregator[Int, Tuple10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple10(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple10(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 11 aggregators" in {
      val agg: Aggregator[Int, Tuple11[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple11(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple11(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 12 aggregators" in {
      val agg: Aggregator[Int, Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple12(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple12(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 13 aggregators" in {
      val agg: Aggregator[Int, Tuple13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple13(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple13(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 14 aggregators" in {
      val agg: Aggregator[Int, Tuple14[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple14(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple14(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 15 aggregators" in {
      val agg: Aggregator[Int, Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple15(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple15(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 16 aggregators" in {
      val agg: Aggregator[Int, Tuple16[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple16(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple16(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 17 aggregators" in {
      val agg: Aggregator[Int, Tuple17[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple17(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple17(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 18 aggregators" in {
      val agg: Aggregator[Int, Tuple18[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple18(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple18(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 19 aggregators" in {
      val agg: Aggregator[Int, Tuple19[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple19(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple19(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 20 aggregators" in {
      val agg: Aggregator[Int, Tuple20[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple20(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple20(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 21 aggregators" in {
      val agg: Aggregator[Int, Tuple21[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple21(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple21(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 22 aggregators" in {
      val agg: Aggregator[Int, Tuple22[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple22(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple22(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }
  }
}
