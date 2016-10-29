package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.prop.Checkers.check
import org.scalacheck.{ Gen, Arbitrary }
import Arbitrary.arbitrary

case class PosNum[T: Numeric](value: T)

object PosNum {
  implicit def arb[T: Numeric: Gen.Choose]: Arbitrary[PosNum[T]] =
    Arbitrary(for { p <- Gen.posNum[T] } yield PosNum(p))
}

case class Tick(count: Long, timestamp: Long)

object Tick {
  implicit val ord: Ordering[Tick] =
    Ordering.by { t: Tick => (t.timestamp, t.count) }

  implicit val arb: Arbitrary[Tick] =
    Arbitrary(for {
      ts <- Gen.posNum[Long]
      tick <- Gen.posNum[Long]
    } yield Tick(tick - 1L, ts))
}

case class NonEmptyList[T](items: List[T]) {
  def sorted(implicit ev: Ordering[T]): List[T] = items.sorted
}

object NonEmptyList {
  implicit def arb[T: Ordering: Arbitrary]: Arbitrary[NonEmptyList[T]] =
    Arbitrary(for {
      l <- Gen.nonEmptyListOf(arbitrary[T])
    } yield NonEmptyList[T](l))
}

class ExpHistLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  implicit val conf: Arbitrary[ExpHist.Config] =
    Arbitrary(for {
      k <- Gen.posNum[Short]
      windowSize <- Gen.posNum[Long]
    } yield ExpHist.Config(k, windowSize))

  def addAll(e: ExpHist, ticks: List[Tick]): ExpHist =
    ticks.foldLeft(e) {
      case (e, Tick(count, timestamp)) =>
        e.add(count, timestamp)
    }

  implicit val expHist: Arbitrary[ExpHist] =
    Arbitrary(for {
      ticks <- arbitrary[List[Tick]]
      conf <- arbitrary[ExpHist.Config]
    } yield addAll(ExpHist.empty(conf), ticks))

  // The example in the paper is actually busted, based on his
  // algorithm. He says to assume that k/2 == 2, but then he promotes
  // as if k/2 == 1, ie k == 2.
  property("example from paper") {
    val e = ExpHist.empty(2, 100)
    val plus76 = e.add(76, 0)

    val inc = plus76.inc(0)
    val incinc = inc.add(2, 0)

    plus76.windows shouldBe Vector(32, 16, 8, 8, 4, 4, 2, 1, 1)
    inc.windows shouldBe Vector(32, 16, 8, 8, 4, 4, 2, 2, 1)
    incinc.windows shouldBe Vector(32, 16, 16, 8, 4, 2, 1)
  }

  property("adding i results in upperBoundSum == i") {
    forAll { (conf: ExpHist.Config, tick: Tick) =>
      assert(ExpHist.empty(conf)
        .add(tick.count, tick.timestamp)
        .upperBoundSum == tick.count)
    }
  }

  property("step should be idempotent") {
    forAll { (expHist: ExpHist, tick: Tick) =>
      val ts = tick.timestamp
      val stepped = expHist.step(ts)
      assert(stepped == stepped.step(ts))

      val added = expHist.add(tick.count, ts)
      val addThenStep = added.step(ts)
      val stepThenAdd = stepped.add(tick.count, ts)

      assert(added == addThenStep)
      assert(added == stepThenAdd)
      assert(addThenStep == stepThenAdd)
    }
  }

  property("stepping is the same as adding 0 at the same ts") {
    forAll { (expHist: ExpHist, ts: PosNum[Long]) =>
      assert(expHist.step(ts.value) == expHist.add(0, ts.value))
    }
  }

  // This is the meat!
  property("bounded error of the query") {
    forAll { (items: NonEmptyList[Tick], conf: ExpHist.Config) =>
      val ticks = items.sorted

      val full = addAll(ExpHist.empty(conf), ticks)

      val finalTs = ticks.last.timestamp

      val actualSum = ticks.collect {
        case Tick(count, ts) if ts > (finalTs - conf.windowSize) => count
      }.sum

      val upperBound = full.upperBoundSum
      val lowerBound = full.lowerBoundSum
      assert(lowerBound <= actualSum)
      assert(upperBound >= actualSum)

      val maxOutsideWindow = full.last - 1
      val minInsideWindow = 1 + full.total - full.last
      val absoluteError = maxOutsideWindow / 2.0
      val relativeError = absoluteError / minInsideWindow
      assert(ExpHist.relativeError(full) <= 1.0 / conf.k)
    }
  }

  property("add and inc should generate the same results") {
    forAll { (tick: Tick, conf: ExpHist.Config) =>
      val e = ExpHist.empty(conf)

      val incs = (0L until tick.count).foldLeft(e) {
        case (acc, _) => acc.inc(tick.timestamp)
      }

      val adds = e.add(tick.count, tick.timestamp)

      assert(incs.total == adds.total)
      assert(incs.lowerBoundSum == adds.lowerBoundSum)
      assert(incs.upperBoundSum == adds.upperBoundSum)
    }
  }

  property("l-canonical representation round-trips") {
    forAll { (i: PosNum[Long], k: PosNum[Short]) =>
      assert(ExpHist.expand(ExpHist.lNormalize(i.value, k.value)) == i.value)
    }
  }

  property("all i except last have either k/2, k/2 + 1 buckets") {
    forAll { (i: PosNum[Long], k: PosNum[Short]) =>
      val lower = ExpHist.minBuckets(k.value)
      val upper = ExpHist.maxBuckets(k.value)
      assert(
        ExpHist.lNormalize(i.value, k.value).init.forall { numBuckets =>
          lower <= numBuckets && numBuckets <= upper
        })
    }
  }
}
