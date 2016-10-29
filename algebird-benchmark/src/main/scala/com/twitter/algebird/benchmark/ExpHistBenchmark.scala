package com.twitter.algebird
package benchmark

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random

/**
 * Benchmarks the Exponential Histogram implementation in Algebird.
 */
object ExpHistBenchmark {
  @State(Scope.Benchmark)
  class ExpHistState {
    @Param(Array("0.1", "0.005"))
    var eps: Double = 0.0

    @Param(Array("1000"))
    var window: Int = 0

    // number of data values to combine into an ExpHist
    @Param(Array("10", "100", "1000"))
    var numElements: Int = 0

    var empty: ExpHist = _
    var inputData: Vector[(Long, Long)] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val rng = new Random(3)

      val conf = ExpHist.Config(math.ceil(1 / eps).toInt, window)
      empty = ExpHist.empty(conf)

      inputData = (0L until numElements).map { _ =>
        val timestamp = rng.nextInt(window).toLong
        val item = rng.nextInt(Int.MaxValue).toLong
        (item, timestamp)
      }.sortBy(_._2).toVector

    }
  }
}

class ExpHistBenchmark {
  import ExpHistBenchmark._

  @Benchmark
  def timeAdd(state: ExpHistState, bh: Blackhole) = {
    state.inputData.foreach { pair =>
      bh.consume(state.empty.add(pair._1, pair._2))
    }
  }
}
