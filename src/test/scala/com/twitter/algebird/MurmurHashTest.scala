package com.twitter.algebird

import java.nio.ByteBuffer

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object MurmurHashableLaws extends Properties("MurmurHashable128")
with BaseProperties {
  property("on bytes matches the CassandraMurmurHash") =
    forAll { (seed: Long, bytes: Array[Byte]) =>
      val ourHash = MurmurHashable128.onByteArray(seed)(bytes)
      val buf = ByteBuffer.wrap(bytes)
      val longs = CassandraMurmurHash.hash3_x64_128(buf, 0, bytes.size, seed)
      ourHash == (longs(0), longs(1))
    }
}
