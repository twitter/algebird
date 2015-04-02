package com.twitter.algebird.caliper

import com.google.caliper.{ SimpleBenchmark, Param }
import com.twitter.algebird.{ HyperLogLogHasAdditionOperatorAndZero, HLL }
import com.twitter.bijection._
import java.nio.ByteBuffer

class HLLPresentBenchmark extends SimpleBenchmark {
  @Param(Array("5", "10", "17", "20"))
  val bits: Int = 0

  @Param(Array("10", "100", "500", "1000", "10000"))
  val max: Int = 0

  @Param(Array("10", "20", "100"))
  val numHLL: Int = 0

  var data: IndexedSeq[HLL] = _

  implicit val byteEncoder = implicitly[Injection[Long, Array[Byte]]]

  override def setUp {
    val hllHasAdditionOperatorAndZero = new HyperLogLogHasAdditionOperatorAndZero(bits)
    val r = new scala.util.Random(12345L)
    data = (0 until numHLL).map { _ =>
      val input = (0 until max).map(_ => r.nextLong).toSet
      hllHasAdditionOperatorAndZero.batchCreate(input)(byteEncoder.toFunction)
    }.toIndexedSeq

  }

  def timeBatchCreate(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      data.foreach { hll =>
        hll.approximateSize
      }
      dummy += 1
    }
    dummy
  }
}
