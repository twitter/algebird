package com.twitter.algebird.benchmark

import com.twitter.algebird.mutable.ReservoirSamplingToListAggregator
import com.twitter.algebird.{Aggregator, Preparer}
import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}
import org.openjdk.jmh.infra.Blackhole

import scala.util.Random

object ReservoirSamplingBenchmark {
  @State(Scope.Benchmark)
  class BenchmarkState {
    @Param(Array("100", "10000", "1000000"))
    var collectionSize: Int = 0

    @Param(Array("0.001", "0.01", "0.1"))
    var sampleRate: Double = 0.0

    def samples: Int = (sampleRate * collectionSize).ceil.toInt
  }

  val rng = new Random()
  implicit val randomSupplier: () => Random = () => rng
}

class ReservoirSamplingBenchmark {
  import ReservoirSamplingBenchmark._

  private def prioQueueSampler[T](count: Int) =
    Preparer[T]
      .map(rng.nextDouble() -> _)
      .monoidAggregate(Aggregator.sortByTake(count)(_._1))
      .andThenPresent(_.map(_._2))

  @Benchmark
  def timeAlgorithmL(state: BenchmarkState, bh: Blackhole): Unit =
    bh.consume(new ReservoirSamplingToListAggregator[Int](state.samples).apply(0 until state.collectionSize))

  @Benchmark
  def timePriorityQeueue(state: BenchmarkState, bh: Blackhole): Unit =
    bh.consume(prioQueueSampler(state.samples).apply(0 until state.collectionSize))
}
