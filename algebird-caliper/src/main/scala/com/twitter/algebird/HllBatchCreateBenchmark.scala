package com.twitter.algebird.caliper

import com.google.caliper.{Param, SimpleBenchmark}
import com.twitter.algebird.{HyperLogLogMonoid, HyperLogLogMonoid2}

import java.nio.ByteBuffer

/**
  * This is the absolute minimal benchmark. It does nothing but time the rep loop.
 */

class HllBatchCreateBenchmark extends SimpleScalaBenchmark {
  @Param(Array("10", "20", "30"))
  val max: Long = 0

  @Param(Array("5", "10", "17", "25"))
  val bits: Int = 0

  var set: Set[Long] = _

  /* Don't use Bijection to reduce dependencies on other projects */
  implicit def injection(value: Long) = {
    val size = 8
    val buf = ByteBuffer.allocate(size)
    buf.putLong(value)
    buf.array
  }

  override def setUp {
    set = (0L until max).toSet

  }

  def timeWithBitSet(reps: Int): Int = repeat(reps) {
    val hllMonoid = new HyperLogLogMonoid(bits)
    var dummy = 0
    while (dummy < reps) {
      val hll = hllMonoid.batchCreate(set)(injection)
      dummy += 1
    }
    dummy
  }

  def timeWithBitSetWrapper(reps: Int): Int = repeat(reps) {
    val hllMonoid = new HyperLogLogMonoid2(bits)
    var dummy = 0
    while (dummy < reps) {
      val hll = hllMonoid.batchCreate(set)(injection)
      dummy += 1
    }
    dummy
  }

}
