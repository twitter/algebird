package com.twitter.algebird

import org.scalatest._

import org.scalacheck.{ Gen, Arbitrary, Properties }

import HyperLogLog.{ int2Bytes, long2Bytes }

class HyperLogLogSeriesLaws extends CheckProperties {
  import BaseProperties._

  implicit val hllSeriesMonoid = new HyperLogLogSeriesMonoid(5) //5 bits

  implicit val hllSeriesGen = Arbitrary {
    for (
      v <- Gen.choose(0, 10000)
    ) yield (hllSeriesMonoid.create(v, v))
  }

  property("HyperLogLogSeries is a Monoid") {
    monoidLawsEq[HLLSeries]{ _.toHLL == _.toHLL }
  }
}

class HLLSeriesSinceProperty extends ApproximateProperty {

  // List of (value, timestamp) pairs
  type Exact = Seq[(Long, Long)]
  type Approx = HLLSeries

  type Input = Long
  type Result = Long

  val bits = 12
  val hllSeriesMonoid = new HyperLogLogSeriesMonoid(bits)
  val hll = new HyperLogLogMonoid(bits)

  def makeApproximate(timestampedData: Seq[(Long, Long)]) = {
    val hllSeries = timestampedData
      .map { case (value, timestamp) => hllSeriesMonoid.create(value, timestamp) }
    hllSeriesMonoid.sum(hllSeries)
  }

  def exactGenerator: Gen[Seq[(Long, Long)]] = for {
    data <- Gen.listOfN(100, Gen.choose(Long.MinValue, Long.MaxValue))
    timestamps = 1L to 100L
  } yield data.zip(timestamps)

  // arbitrary timestamp
  def inputGenerator(timestampedData: Exact): Gen[Long] =
    Gen.oneOf(timestampedData).map { case (value, timestamp) => timestamp }

  def approximateResult(series: HLLSeries, timestamp: Long) =
    series.since(timestamp).toHLL.approximateSize

  def exactResult(timestampedData: Seq[(Long, Long)], timestamp: Long): Long =
    timestampedData
      .dropWhile { case (_, t) => t < timestamp }
      .map { case (value, _) => value }
      .length
}

class HLLSeriesProperties extends Properties("HyperLogLogSeries") {
  import ApproximateProperty.toProp

  property("properly calculates .since") =
    toProp(new HLLSeriesSinceProperty, 1, 100, 0.01)
}
