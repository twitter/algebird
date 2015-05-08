package com.twitter.algebird

import org.scalatest._

import org.scalacheck.{ Gen, Arbitrary }

import HyperLogLog._ //Get the implicit int2bytes, long2Bytes

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

class HyperLogLogSeriesTest extends WordSpec with Matchers {
  def getHllCount[T <% Array[Byte]](it: Iterable[T], hll: HyperLogLogMonoid) = {
    hll.sizeOf(hll.sum(it.map { hll(_) })).estimate.toDouble
  }

  def aveErrorOf(bits: Int): Double = 1.04 / scala.math.sqrt(1 << bits)

  def testApproximatelyEqual(hllSeries: HLLSeries, hllCount: Double, bits: Int) = {
    val seriesResult = hllSeries.toHLL.estimatedSize
    assert(scala.math.abs(seriesResult - hllCount) / seriesResult < (3.5 * aveErrorOf(bits)))
  }

  "HyperLogLogSeries" should {
    "properly calculate .since" in {
      val bits = 12
      val hllSeriesMonoid = new HyperLogLogSeriesMonoid(bits)
      val hll = new HyperLogLogMonoid(bits)

      val timestamps = (1 to 100).map { _.toLong }
      val r = new java.util.Random
      val timestampedData = timestamps.map { t => (r.nextLong, t) }

      val series = timestampedData
        .map{ case (value, timestamp) => hllSeriesMonoid.create(value, timestamp) }
        .reduce{ hllSeriesMonoid.plus(_, _) }

      timestamps.foreach { timestamp =>
        val seriesResult = series.since(timestamp)

        val dataSinceTimestamp = timestampedData
          .dropWhile { case (_, t) => t < timestamp }
          .map { case (value, _) => value }
        val expected = getHllCount(dataSinceTimestamp, hll)

        testApproximatelyEqual(seriesResult, expected, bits)
      }
    }
  }
}
