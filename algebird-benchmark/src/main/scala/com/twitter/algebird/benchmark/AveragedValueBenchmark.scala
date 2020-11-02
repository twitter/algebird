package com.twitter.algebird
package benchmark

import scala.util.Random
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object AveragedValueBenchmark {
  @State(Scope.Benchmark)
  class AVState {
    @Param(Array("10000"))
    var numElements: Int = 0

    var inputData: Seq[AveragedValue] = _

    @Setup(Level.Trial)
    def setup(): Unit =
      inputData = Seq.fill(numElements)(AveragedValue(Random.nextInt(1000).toLong))
  }
}

class AveragedValueBenchmark {
  import AveragedValueBenchmark._
  import AveragedGroup.{plus, sumOption}

  @Benchmark
  def timePlus(state: AVState, bh: Blackhole): Unit =
    bh.consume(state.inputData.reduce(plus(_, _)))

  @Benchmark
  def timeSumOption(state: AVState, bh: Blackhole): Unit =
    bh.consume(sumOption(state.inputData))
}
