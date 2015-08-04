package com.twitter.algebird.benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.bijection._

object HllBatchCreateBenchmark {
  val byteEncoder = implicitly[Injection[Long, Array[Byte]]]
  val byteEncoderFn = byteEncoder.toFunction

  @State(Scope.Benchmark)
  class HLLState {

    @Param(Array("5", "10", "17", "25"))
    var bits: Int = 0

    @Param(Array("10", "20", "30"))
    var max: Long = 0

    var set: Set[Long] = _

    var hllMonoid: HyperLogLogMonoid = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      hllMonoid = new HyperLogLogMonoid(bits)

      set = (0L until max).toSet
    }
  }
}

class HllBatchCreateBenchmark {
  import HllBatchCreateBenchmark._

  @Benchmark
  def timeBatchCreate(state: HLLState) =
    state.hllMonoid.batchCreate(state.set)(byteEncoderFn)
}
