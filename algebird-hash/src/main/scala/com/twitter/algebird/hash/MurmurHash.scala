package com.twitter.algebird.hash

import java.nio.ByteBuffer

object Murmur {
  val defaultSeed = 0L // There doesn't appear to be any risk of choosing any particular seed
  // Compiler should remove these, just here for consistency:

  /** Murmur2 32 bits. Takes a long seed for consistency with the others.
   */
  def hash2_32(data: ByteBuffer, offset: Int, length: Int, seed: Long): Int =
    CassandraMurmurHash.hash32(data, offset, length, (seed >> 32).toInt ^ (seed.toInt))

  def hash2_64(data: ByteBuffer, offset: Int, length: Int, seed: Long): Long =
    CassandraMurmurHash.hash2_64(data, offset, length, seed)

  def hash3_128(buffer: ByteBuffer, offset : Int, length : Int, seed: Long): (Long,Long) = {
    val longs = CassandraMurmurHash.hash3_x64_128(buffer, offset, length, seed)
    (longs(0), longs(1))
  }
  /** Returns a hashable that hashes the entire ByteBuffer, from position to limit
   */
  def hash2Int(seed: Long = defaultSeed): Hashable[ByteBuffer, Int] =
    new Hashable[ByteBuffer, Int] {
      def apply(input: ByteBuffer) =
        Murmur.hash2_32(input, input.position, input.limit, seed)
    }
  def hash2Long(seed: Long = defaultSeed): Hashable[ByteBuffer, Long] =
    new Hashable[ByteBuffer, Long] {
      def apply(input: ByteBuffer) =
        Murmur.hash2_64(input, input.position, input.limit, seed)
    }
  def hash3(seed: Long = defaultSeed): Hashable[ByteBuffer,(Long,Long)] =
    new Hashable[ByteBuffer, (Long,Long)] {
      def apply(input: ByteBuffer) =
        Murmur.hash3_128(input, input.position, input.limit, seed)
    }
}
