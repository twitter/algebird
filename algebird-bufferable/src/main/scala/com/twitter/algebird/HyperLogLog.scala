package com.twitter.algebird

import com.twitter.bijection._

object HyperLogLogBijection extends AbstractBijection[HLL, Array[Byte]] {
  override def apply(hll : HLL) = HyperLogLog.toBytes(hll)
  override def invert(bytes : Array[Byte]) = HyperLogLog.fromBytes(bytes)
}