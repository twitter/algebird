package com.twitter.algebird.benchmark

import com.twitter.algebird.{DenseHLL, HLL, HyperLogLogMonoid, SparseHLL}
import com.twitter.bijection._
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object HLLPresentBenchmark {
  implicit val byteEncoder = implicitly[Injection[Long, Array[Byte]]]

  @State(Scope.Benchmark)
  class HLLPresentState {

    @Param(Array("5", "10", "17", "20"))
    var bits: Int = 0

    @Param(Array("10", "100", "500", "1000", "10000"))
    var max: Int = 0

    @Param(Array("10", "20", "100"))
    var numHLL: Int = 0

    var data: IndexedSeq[HLL] = _
    @Setup(Level.Trial)
    def setup(): Unit = {
      val hllMonoid = new HyperLogLogMonoid(bits)
      val r = new scala.util.Random(12345L)
      data = (0 until numHLL).map { _ =>
        val input = (0 until max).map(_ => r.nextLong).toSet
        hllMonoid.batchCreate(input)(byteEncoder.toFunction)
      }.toIndexedSeq
    }
  }
}

class HLLPresentBenchmark {
  import HLLPresentBenchmark._

  //don't cache the lazy values
  def clone(hll: HLL): HLL =
    hll match {
      case SparseHLL(bits, maxRhow) => SparseHLL(bits, maxRhow)
      case DenseHLL(bits, v)        => DenseHLL(bits, v)
    }

  @Benchmark
  def timeBatchCreate(state: HLLPresentState, bh: Blackhole) =
    state.data.foreach { hll =>
      bh.consume(clone(hll).approximateSize)
    }
}
