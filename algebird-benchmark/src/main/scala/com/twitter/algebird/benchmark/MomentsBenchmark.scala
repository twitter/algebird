package com.twitter.algebird
package benchmark

import scala.util.Random

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object MomentsBenchmark {
  @State(Scope.Benchmark)
  class MomentsState {
    @Param(Array("10000"))
    var numElements: Int = 0

    var inputData: Seq[Double] = _
    var inputMoments: Seq[Moments] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      inputData = Seq.fill(numElements)(Random.nextInt(1000).toLong)
      inputMoments = inputData.map(Moments(_))
    }
  }
}

class MomentsBenchmark {
  import MomentsBenchmark._

  @Benchmark
  def timePlusDoubles(state: MomentsState, bh: Blackhole): Unit =
    bh.consume(
      state.inputData.foldLeft(Moments.momentsMonoid.zero)(_ + _)
    )

  @Benchmark
  def timePlusMoments(state: MomentsState, bh: Blackhole): Unit =
    bh.consume(
      state.inputMoments.foldLeft(Moments.momentsMonoid.zero)(_ + _)
    )

  @Benchmark
  def timeSumOption(state: MomentsState, bh: Blackhole): Unit =
    bh.consume(Moments.momentsMonoid.sumOption(state.inputMoments))

  @Benchmark
  def timeFold(state: MomentsState, bh: Blackhole): Unit =
    bh.consume(Moments.fold.overTraversable(state.inputData))

  @Benchmark
  def timeAggregate(state: MomentsState, bh: Blackhole): Unit =
    bh.consume(Moments.aggregator(state.inputData))
}
