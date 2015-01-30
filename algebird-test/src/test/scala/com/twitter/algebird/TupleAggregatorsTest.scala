package com.twitter.algebird

import org.scalatest._

class TupleAggregatorsTest extends WordSpec with Matchers {
  // This gives you an implicit conversion from tuples of aggregators
  // to aggregator of tuples

  val data = List(1, 3, 2, 0, 5, 6)
  val MinAgg = Aggregator.min[Int]

  "GeneratedTupleAggregators" should {
    import GeneratedTupleAggregator._

    "Create an aggregator from a tuple of 2 aggregators" in {
      val agg: Aggregator[Int, Tuple2[Int, Int], Tuple2[Int, Int]] = Tuple2(MinAgg, MinAgg)
      assert(agg(data) == Tuple2(0, 0))
    }

    "Create an aggregator from a tuple of 3 aggregators" in {
      val agg: Aggregator[Int, Tuple3[Int, Int, Int], Tuple3[Int, Int, Int]] = Tuple3(MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple3(0, 0, 0))
    }

    "Create an aggregator from a tuple of 4 aggregators" in {
      val agg: Aggregator[Int, Tuple4[Int, Int, Int, Int], Tuple4[Int, Int, Int, Int]] = Tuple4(MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple4(0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 5 aggregators" in {
      val agg: Aggregator[Int, Tuple5[Int, Int, Int, Int, Int], Tuple5[Int, Int, Int, Int, Int]] = Tuple5(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple5(0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 6 aggregators" in {
      val agg: Aggregator[Int, Tuple6[Int, Int, Int, Int, Int, Int], Tuple6[Int, Int, Int, Int, Int, Int]] = Tuple6(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple6(0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 7 aggregators" in {
      val agg: Aggregator[Int, Tuple7[Int, Int, Int, Int, Int, Int, Int], Tuple7[Int, Int, Int, Int, Int, Int, Int]] = Tuple7(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple7(0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 8 aggregators" in {
      val agg: Aggregator[Int, Tuple8[Int, Int, Int, Int, Int, Int, Int, Int], Tuple8[Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple8(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple8(0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 9 aggregators" in {
      val agg: Aggregator[Int, Tuple9[Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple9[Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple9(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple9(0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 10 aggregators" in {
      val agg: Aggregator[Int, Tuple10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple10(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple10(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 11 aggregators" in {
      val agg: Aggregator[Int, Tuple11[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple11[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple11(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple11(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 12 aggregators" in {
      val agg: Aggregator[Int, Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple12(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple12(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 13 aggregators" in {
      val agg: Aggregator[Int, Tuple13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple13(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple13(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 14 aggregators" in {
      val agg: Aggregator[Int, Tuple14[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple14[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple14(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple14(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 15 aggregators" in {
      val agg: Aggregator[Int, Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple15(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple15(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 16 aggregators" in {
      val agg: Aggregator[Int, Tuple16[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple16[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple16(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple16(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 17 aggregators" in {
      val agg: Aggregator[Int, Tuple17[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple17[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple17(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple17(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 18 aggregators" in {
      val agg: Aggregator[Int, Tuple18[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple18[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple18(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple18(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 19 aggregators" in {
      val agg: Aggregator[Int, Tuple19[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple19[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple19(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple19(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 20 aggregators" in {
      val agg: Aggregator[Int, Tuple20[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple20[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple20(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple20(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 21 aggregators" in {
      val agg: Aggregator[Int, Tuple21[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple21[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple21(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple21(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 22 aggregators" in {
      val agg: Aggregator[Int, Tuple22[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple22[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = Tuple22(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple22(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }
  }

  "MultiAggregator" should {
    import MultiAggregator._

    "Create an aggregator from a tuple of 2 aggregators" in {
      val agg: Aggregator[Int, Tuple2[Int, Int], Tuple2[Int, Int]] = MultiAggregator(MinAgg, MinAgg)
      assert(agg(data) == Tuple2(0, 0))
    }

    "Create an aggregator from a tuple of 3 aggregators" in {
      val agg: Aggregator[Int, Tuple3[Int, Int, Int], Tuple3[Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple3(0, 0, 0))
    }

    "Create an aggregator from a tuple of 4 aggregators" in {
      val agg: Aggregator[Int, Tuple4[Int, Int, Int, Int], Tuple4[Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple4(0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 5 aggregators" in {
      val agg: Aggregator[Int, Tuple5[Int, Int, Int, Int, Int], Tuple5[Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple5(0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 6 aggregators" in {
      val agg: Aggregator[Int, Tuple6[Int, Int, Int, Int, Int, Int], Tuple6[Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple6(0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 7 aggregators" in {
      val agg: Aggregator[Int, Tuple7[Int, Int, Int, Int, Int, Int, Int], Tuple7[Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple7(0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 8 aggregators" in {
      val agg: Aggregator[Int, Tuple8[Int, Int, Int, Int, Int, Int, Int, Int], Tuple8[Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple8(0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 9 aggregators" in {
      val agg: Aggregator[Int, Tuple9[Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple9[Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple9(0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 10 aggregators" in {
      val agg: Aggregator[Int, Tuple10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple10[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple10(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 11 aggregators" in {
      val agg: Aggregator[Int, Tuple11[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple11[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple11(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 12 aggregators" in {
      val agg: Aggregator[Int, Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple12(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 13 aggregators" in {
      val agg: Aggregator[Int, Tuple13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple13(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 14 aggregators" in {
      val agg: Aggregator[Int, Tuple14[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple14[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple14(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 15 aggregators" in {
      val agg: Aggregator[Int, Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple15(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 16 aggregators" in {
      val agg: Aggregator[Int, Tuple16[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple16[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple16(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 17 aggregators" in {
      val agg: Aggregator[Int, Tuple17[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple17[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple17(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 18 aggregators" in {
      val agg: Aggregator[Int, Tuple18[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple18[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple18(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 19 aggregators" in {
      val agg: Aggregator[Int, Tuple19[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple19[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple19(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 20 aggregators" in {
      val agg: Aggregator[Int, Tuple20[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple20[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple20(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 21 aggregators" in {
      val agg: Aggregator[Int, Tuple21[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple21[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple21(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    "Create an aggregator from a tuple of 22 aggregators" in {
      val agg: Aggregator[Int, Tuple22[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int], Tuple22[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]] = MultiAggregator(MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg, MinAgg)
      assert(agg(data) == Tuple22(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    val longData = data.map{ _.toLong }
    val MinMonoidAgg = Aggregator.size

    "Create a MonoidAggregator from a tuple of 2 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple2[Long, Long], Tuple2[Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple2(6, 6))
    }

    "Create a MonoidAggregator from a tuple of 3 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple3[Long, Long, Long], Tuple3[Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple3(6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 4 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple4[Long, Long, Long, Long], Tuple4[Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple4(6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 5 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple5[Long, Long, Long, Long, Long], Tuple5[Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple5(6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 6 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple6[Long, Long, Long, Long, Long, Long], Tuple6[Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple6(6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 7 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple7[Long, Long, Long, Long, Long, Long, Long], Tuple7[Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple7(6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 8 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple8[Long, Long, Long, Long, Long, Long, Long, Long], Tuple8[Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple8(6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 9 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple9[Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple9[Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple9(6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 10 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple10[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple10[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple10(6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 11 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple11[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple11[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple11(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 12 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple12[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple12[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple12(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 13 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple13[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple13[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple13(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 14 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple14[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple14[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple14(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 15 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple15[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple15[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple15(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 16 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple16[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple16[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple16(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 17 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple17[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple17[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple17(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 18 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple18[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple18[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple18(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 19 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple19[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple19[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple19(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 20 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple20[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple20[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple20(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 21 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple21[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple21[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple21(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }

    "Create a MonoidAggregator from a tuple of 22 MonoidAggregators" in {
      val agg: MonoidAggregator[Long, Tuple22[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long], Tuple22[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]] = MultiAggregator(MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg, MinMonoidAgg)
      assert(agg(longData) == Tuple22(6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6))
    }
  }
}
