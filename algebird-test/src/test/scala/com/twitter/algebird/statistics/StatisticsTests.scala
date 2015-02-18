package com.twitter.algebird.statistics

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Arbitrary, Properties }
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Gen._
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import com.twitter.algebird.BaseProperties

class StatisticsRingLaws extends PropSpec with PropertyChecks with ShouldMatchers {
  import BaseProperties._

  val statsRing = new StatisticsRing[Int]
  val gen = for (v <- choose(0, 1 << 30)) yield v

  property("StatisticsRing is a Ring") {
    ringLaws[Int](statsRing, Arbitrary(gen))
  }

}

class StatisticsMonoidLaws extends PropSpec with PropertyChecks with ShouldMatchers {
  import BaseProperties._

  val statsMonoid = new StatisticsMonoid[Int]

  val gen = for (v <- choose(0, 1 << 14)) yield v

  property("StatisticsMonoid is a Monoid") {
    monoidLaws[Int](statsMonoid, Arbitrary(gen))
  }

}

class StatisticsTest extends WordSpec with ShouldMatchers {

  // the test framework garbles the exceptions :/
  lazy val statsMonoid = new StatisticsMonoid[Int]
  try {
    for (i <- 1 to 2) statsMonoid.zero
    for (i <- 1 to 3) statsMonoid.plus(i, i)
    for (i <- 1 to 3000) statsMonoid.sum(for (v <- 1 to i) yield v)
    for (i <- 1 to 2000) statsMonoid.sumOption(for (v <- 1 to i) yield v)
  } catch {
    case e: Exception => {
      e.printStackTrace()
      throw e
    }
  }

  "StatisticsMonoid" should {

    "count zero calls" in {
      assert(statsMonoid.getZeroCallCount == 2)
    }

    "count plus calls" in {
      assert(statsMonoid.getPlusCallCount == 3)
    }

    "count sum calls" in {
      assert(statsMonoid.getSumCallCount == 3000)
      assert(statsMonoid.getSumCallTime > 0L)
      statsMonoid.toString.contains("sum calls: <1: 0, <2: 1, <4: 2, <8: 4, <16: 8, <32: 16, <64: 32, <128: 64, <256: 128, <512: 256, >: 2489, avg=1500.5 count=3000")
    }

    "count sumOption calls" in {
      assert(statsMonoid.getSumOptionCallCount == 2000)
      assert(statsMonoid.getSumOptionCallTime > 0L)
      statsMonoid.toString.contains("sumOption calls: <1: 0, <2: 1, <4: 2, <8: 4, <16: 8, <32: 16, <64: 32, <128: 64, <256: 128, <512: 256, >: 1489, avg=1000.5 count=2000")
    }

  }

}
