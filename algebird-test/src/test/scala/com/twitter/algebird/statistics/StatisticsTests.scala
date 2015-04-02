package com.twitter.algebird.statistics

import com.twitter.algebird.CheckProperties
import org.scalacheck.Arbitrary
import org.scalacheck.Gen._
import org.scalatest.{ Matchers, _ }

class StatisticsRingLaws extends CheckProperties with Matchers {
  import com.twitter.algebird.BaseProperties._

  val statsRing = new StatisticsRing[Int]
  val gen = for (v <- choose(0, 1 << 30)) yield v

  property("StatisticsRing is a Ring") {
    ringLaws[Int](statsRing, Arbitrary(gen))
  }

}

class StatisticsHasAdditionOperatorAndZeroLaws extends CheckProperties with Matchers {
  import com.twitter.algebird.BaseProperties._

  val statsHasAdditionOperatorAndZero = new StatisticsHasAdditionOperatorAndZero[Int]

  val gen = for (v <- choose(0, 1 << 14)) yield v

  property("StatisticsHasAdditionOperatorAndZero is a HasAdditionOperatorAndZero") {
    monoidLaws[Int](statsHasAdditionOperatorAndZero, Arbitrary(gen))
  }

}

class StatisticsTest extends WordSpec with Matchers {

  // the test framework garbles the exceptions :/
  lazy val statsHasAdditionOperatorAndZero = new StatisticsHasAdditionOperatorAndZero[Int]
  try {
    for (i <- 1 to 2) statsHasAdditionOperatorAndZero.zero
    for (i <- 1 to 3) statsHasAdditionOperatorAndZero.plus(i, i)
    for (i <- 1 to 3000) statsHasAdditionOperatorAndZero.sum(for (v <- 1 to i) yield v)
    for (i <- 1 to 2000) statsHasAdditionOperatorAndZero.sumOption(for (v <- 1 to i) yield v)
  } catch {
    case e: Exception => {
      e.printStackTrace()
      throw e
    }
  }

  "StatisticsHasAdditionOperatorAndZero" should {

    "count zero calls" in {
      assert(statsHasAdditionOperatorAndZero.getZeroCallCount == 2)
    }

    "count plus calls" in {
      assert(statsHasAdditionOperatorAndZero.getPlusCallCount == 3)
    }

    "count sum calls" in {
      assert(statsHasAdditionOperatorAndZero.getSumCallCount == 3000)
      assert(statsHasAdditionOperatorAndZero.getSumCallTime > 0L)
      statsHasAdditionOperatorAndZero.toString.contains("sum calls: <1: 0, <2: 1, <4: 2, <8: 4, <16: 8, <32: 16, <64: 32, <128: 64, <256: 128, <512: 256, >: 2489, avg=1500.5 count=3000")
    }

    "count sumOption calls" in {
      assert(statsHasAdditionOperatorAndZero.getSumOptionCallCount == 2000)
      assert(statsHasAdditionOperatorAndZero.getSumOptionCallTime > 0L)
      statsHasAdditionOperatorAndZero.toString.contains("sumOption calls: <1: 0, <2: 1, <4: 2, <8: 4, <16: 8, <32: 16, <64: 32, <128: 64, <256: 128, <512: 256, >: 1489, avg=1000.5 count=2000")
    }

  }

}
