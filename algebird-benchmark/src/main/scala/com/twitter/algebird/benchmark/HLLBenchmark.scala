package com.twitter.algebird
package benchmark

import scala.util.Random
import com.twitter.bijection._

import com.twitter.algebird.util._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.math._

class OldMonoid(bits: Int) extends HyperLogLogMonoid(bits) {
  import HyperLogLog._

  override def sumOption(items: TraversableOnce[HLL]): Option[HLL] =
    if (items.isEmpty) None
    else {
      val buffer = new Array[Byte](size)
      items.foreach { _.updateInto(buffer) }
      Some(DenseHLL(bits, Bytes(buffer)))
    }
}

object HllBenchmark {

  @State(Scope.Benchmark)
  class HLLState {
    var hllMonoid: HyperLogLogMonoid = _
    var oldHllMonoid: HyperLogLogMonoid = _

    @Param(Array("12", "14", "24"))
    var numBits: Int = 0

    // Old sum option will not work with >=100 keys, and >= 1000 elements.
    @Param(Array("1", "10"))
    var numInputKeys: Int = 0

    @Param(Array("10", "1000", "10000"))
    var numElements: Int = 0

    var inputData: Seq[Seq[HLL]] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      hllMonoid = new HyperLogLogMonoid(numBits)

      oldHllMonoid = new OldMonoid(numBits)

      val rng = new Random(3)

      val byteEncoder = implicitly[Injection[Long, Array[Byte]]]
      def setSize = rng.nextInt(10) + 1 // 1 -> 10
      def hll(elements: Set[Long]): HLL = hllMonoid.batchCreate(elements)(byteEncoder)

      val inputIntermediate = (0L until numElements).map { _ =>
        val setElements = (0 until setSize).map{ _ => rng.nextInt(1000).toLong }.toSet
        (pow(numInputKeys, rng.nextFloat).toLong, List(hll(setElements)))
      }
      inputData = MapAlgebra.sumByKey(inputIntermediate).map(_._2).toSeq
    }
  }
}

class HllBenchmark {
  import HllBenchmark._
  @Benchmark
  def timeSumOption(state: HLLState, bh: Blackhole) = {
    state.inputData.foreach { vals =>
      bh.consume(state.hllMonoid.sumOption(vals))
    }

  }

  @Benchmark
  def timeOldSumOption(state: HLLState, bh: Blackhole) = {
    state.inputData.foreach { d =>
      bh.consume(state.oldHllMonoid.sumOption(d))
    }
  }

  @Benchmark
  def timePlus(state: HLLState, bh: Blackhole) = {
    state.inputData.foreach { vals =>
      bh.consume(vals.reduce(state.hllMonoid.plus(_, _)))
    }
  }
}
