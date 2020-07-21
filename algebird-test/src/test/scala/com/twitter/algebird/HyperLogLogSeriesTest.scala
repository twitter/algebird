package com.twitter.algebird

import org.scalacheck.{Arbitrary, Gen, Prop}
import Arbitrary.arbitrary

import HyperLogLog.{int2Bytes, long2Bytes}

class HyperLogLogSeriesLaws extends CheckProperties {
  import BaseProperties._

  implicit val monoid: HyperLogLogSeriesMonoid = new HyperLogLogSeriesMonoid(8)

  case class Timestamp(value: Long)

  object Timestamp {
    implicit val arbitraryTimestamp: Arbitrary[Timestamp] =
      Arbitrary(Gen.choose(0L, 1000000L).map(t => Timestamp(t)))
  }

  def absorb(h0: HLLSeries, ts: List[Timestamp]): HLLSeries =
    monoid.sum(h0 :: ts.map(t => monoid.create(long2Bytes(t.value), t.value)))

  def directAbsorb(h0: HLLSeries, ts: List[Timestamp]): HLLSeries =
    ts.foldLeft(h0)((h, t) => h.insert(long2Bytes(t.value), t.value))

  implicit val hllSeriesGen: Arbitrary[HLLSeries] =
    Arbitrary(arbitrary[List[Timestamp]].map(ts => absorb(monoid.zero, ts)))

  property("HyperLogLogSeries is a Monoid") {
    commutativeMonoidLaws[HLLSeries]
  }

  property("HyperLogLogSeries is commutative") {
    Prop.forAll((h: HLLSeries, ts: List[Timestamp]) => absorb(h, ts) == absorb(h, ts.reverse))
  }

  property("series.approximateSizeSince(start) = h.since(t).toHLL.approximateSize") {
    Prop.forAll { (h: HLLSeries, t: Timestamp) =>
      h.approximateSizeSince(t.value) == h.since(t.value).toHLL.approximateSize
    }
  }

  property("h.insert(bs, t) = m.plus(h, m.create(bs, t))") {
    Prop.forAll((h: HLLSeries, ts: List[Timestamp]) => absorb(h, ts) == directAbsorb(h, ts))
  }

  // this is a deterministic test to ensure that our rates are staying
  // within the expected error bounds.
  property("verify error rate") {

    // Ensure that building an HLLSeries containing the given
    // cardinality of items have an acceptable error rate.
    def verify(cardinality: Int, errorPct: Double): Boolean = {
      val it = (0 until cardinality).iterator
      val h = monoid.sum(it.map(i => monoid.create(int2Bytes(i), i)))
      val n = h.since(0L).toHLL.approximateSize.estimate
      val delta = (cardinality * errorPct).toInt
      (cardinality - delta) <= n && n <= (cardinality + delta)
    }

    // We've verified that at 8-bits, the follow cardinalities all
    // have <= 10% error. This is intended to protect us against
    // possible future regressions (where the error rate gets worse
    // than expected).
    val cardinalities = List(1024, 2048, 4096, 8192, 16384, 32768, 65536)
    cardinalities.forall(n => verify(n, 0.1))
  }
}

class HLLSeriesSinceProperty extends ApproximateProperty {

  // List of (value, timestamp) pairs
  type Exact = Seq[(Long, Long)]
  type Approx = HLLSeries

  type Input = Long
  type Result = Long

  val bits = 12
  val monoid = new HyperLogLogSeriesMonoid(bits)
  val hll = new HyperLogLogMonoid(bits)

  def makeApproximate(timestampedData: Seq[(Long, Long)]): HLLSeries = {
    val hllSeries = timestampedData
      .map { case (value, timestamp) => monoid.create(value, timestamp) }
    monoid.sum(hllSeries)
  }

  def exactGenerator: Gen[Seq[(Long, Long)]] =
    for {
      data <- Gen.listOfN(100, Gen.choose(Long.MinValue, Long.MaxValue))
      timestamps = 1L to 100L
    } yield data.zip(timestamps)

  // arbitrary timestamp
  def inputGenerator(timestampedData: Exact): Gen[Long] =
    Gen.oneOf(timestampedData).map { case (_, timestamp) => timestamp }

  def approximateResult(series: HLLSeries, timestamp: Long): Approximate[Long] =
    series.since(timestamp).toHLL.approximateSize

  def exactResult(timestampedData: Seq[(Long, Long)], timestamp: Long): Long =
    timestampedData
      .dropWhile { case (_, t) => t < timestamp }
      .map { case (value, _) => value }
      .length
}

class HLLSeriesProperties extends ApproximateProperties("HyperLogLogSeries") {
  import ApproximateProperty.toProp

  property("properly calculates .since") = toProp(new HLLSeriesSinceProperty, 1, 100, 0.01)
}
