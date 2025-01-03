package com.twitter.algebird.mutable

import com.twitter.algebird.RandomSamplingLaws._
import com.twitter.algebird.scalacheck.Distribution.{forAllSampled, uniform}
import com.twitter.algebird.{Aggregator, CheckProperties, Preparer}
import org.scalacheck.Gen

import scala.util.Random

// These tests are probabilistic and expected to fail at a rate of 0.1% per example (multiple examples are run per test)
class ReservoirSamplingTest extends CheckProperties {

  val rng = new Random()
  implicit val randomSupplier: () => Random = () => rng

  property("reservoir sampling with Algorithm L works") {
    randomSamplingDistributions(new ReservoirSamplingToListAggregator[Int](_))
  }

  private def prioQueueSampler[T](count: Int) =
    Preparer[T]
      .map(rng.nextDouble() -> _)
      .monoidAggregate(Aggregator.sortByTake(count)(_._1))
      .andThenPresent(_.map(_._2))

  property("reservoir sampling with priority queue works") {
    randomSamplingDistributions(prioQueueSampler)
  }

  property("sampling from non-indexed Seq") {
    val n = 100
    forAllSampled(10000, Gen.choose(1, 20))(_ => uniform(n)) { k =>
      new ReservoirSamplingToListAggregator[Int](k).apply((0 until n).asInstanceOf[Seq[Int]]).head
    }
  }

  property("append piecewise (Seq)") {
    forAllSampled(10000, Gen.choose(1, 20))(k => uniform(k + 1)) { k =>
      val agg = new ReservoirSamplingToListAggregator[Int](k)
      val r1 = agg.appendAll(agg.monoid.zero, 0 until k)
      val r2 = agg.append(r1, k)
      agg.present(r2).head
    }
  }

  property("append piecewise (IndexedSeq)") {
    forAllSampled(10000, Gen.choose(1, 20))(k => uniform(k + 1)) { k =>
      val agg = new ReservoirSamplingToListAggregator[Int](k)
      val r1 = agg.appendAll(agg.monoid.zero, 0 until k)
      val r2 = agg.appendAll(r1, IndexedSeq(k))
      agg.present(r2).head
    }
  }

  property("append piecewise 2 (IndexedSeq)") {
    forAllSampled(10000, Gen.choose(1, 20), Gen.choose(1, 5), Gen.choose(1, 5))((k, l, m) =>
      uniform(k + l + m)
    ) { (k, l, m) =>
      val agg = new ReservoirSamplingToListAggregator[Int](k)
      val r1 = agg.appendAll(agg.monoid.zero, 0 until k)
      val r2 = agg.appendAll(r1, k until k + l)
      val r3 = agg.appendAll(r2, k + l until k + l + m)
      agg.present(r3).head
    }
  }
}
