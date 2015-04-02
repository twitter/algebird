package com.twitter.algebird.caliper

import com.google.caliper.{ SimpleBenchmark, Param }
import com.twitter.algebird.HyperLogLogHasAdditionOperatorAndZero
import com.twitter.bijection._
import java.nio.ByteBuffer

class HllBatchCreateBenchmark extends SimpleBenchmark {
  @Param(Array("5", "10", "17", "25"))
  val bits: Int = 0

  @Param(Array("10", "20", "30"))
  val max: Long = 0

  var set: Set[Long] = _

  implicit val byteEncoder = implicitly[Injection[Long, Array[Byte]]]

  override def setUp {
    set = (0L until max).toSet
  }

  def timeBatchCreate(reps: Int): Int = {
    val hllHasAdditionOperatorAndZero = new HyperLogLogHasAdditionOperatorAndZero(bits)
    var dummy = 0
    while (dummy < reps) {
      val hll = hllHasAdditionOperatorAndZero.batchCreate(set)(byteEncoder.toFunction)
      dummy += 1
    }
    dummy
  }
}
