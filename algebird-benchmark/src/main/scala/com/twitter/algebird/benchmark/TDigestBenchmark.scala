package com.twitter.algebird.benchmark

import com.twitter.algebird._
import com.twitter.bijection._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random

object TDigestBenchmark {
  import org.apache.commons.math3.distribution.NormalDistribution

  @State(Scope.Benchmark)
  class TDigestState {
    @Param(Array("0.5", "0.05"))
    var delta: Double = 0.0

    @Param(Array("10000", "100000"))
    var sampleSize: Int = 0

    var data: Seq[Double] = _
    var tde: TDigest = _
    var agg: TDigestAggregator[Double] = _

    @Setup(Level.Trial)
    def setup() {
      val dist = new NormalDistribution()
      data = Vector.fill(sampleSize) { dist.sample }
      tde = TDigest.empty(delta)
      agg = TDigest.aggregator[Double](delta)
    }
  }
}

@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class TDigestBenchmark {
  import TDigestBenchmark._

  @Benchmark
  def sketchWithTDigest(state: TDigestState) = {
    state.data.foldLeft(state.tde)((td, e) => td + e)
  }

  @Benchmark
  def sketchWithTDigestAggregator(state: TDigestState) = {
    state.agg(state.data)
  }
}
