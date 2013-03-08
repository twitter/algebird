package com.twitter.algebird.hash

import java.nio._

object Murmur {
  val defaultSeed = 0L // There doesn't appear to be any risk of choosing any particular seed
  // Compiler will remove these, just here for consistency:

  /** Murmur2 32 bits. Takes a long seed for consistency with the others.
   */
  def hash2_32(data: ByteBuffer, offset: Int, length: Int, seed: Long): Int =
    CassandraMurmurHash.hash32(data, offset, length, (seed >> 32).toInt ^ (seed.toInt))

  def hash2_64(data: ByteBuffer, offset: Int, length: Int, seed: Long): Long =
    CassandraMurmurHash.hash2_64(ByteBuffer key, int offset, int length, long seed)

  def hash3_128(buffer : ByteBuffer, offset : Int, length : Int, seed: Long): (Long,Long) = {
    val longs = CassandraMurmurHash.hash3_x64_128(buffer, offset, length, seed)
    (longs(0), longs(1))
  }
}

case class MurmurHash128(seed : Long) { self =>

  lazy val byteBuffer: Hashable[(ByteBuffer,Int,Int), (Long,Long)] = {
    def apply(input: (ByteBuffer, Int, Int)) = Murmur.hash3_128(input._1, input._2, input._3)
  }

  lazy val bytes: Hashable[Array[Byte], (Long,Long)] = {
    def apply(b: Array[Byte]) = self.apply(ByteBuffer.wrap(b), 0, b.length)
  }

  def apply(b : Array[Byte]) : (Long, Long) = bytes(b)

  def apply(maxBytes : Int, fn : ByteBuffer => Unit) : (Long, Long) = {
    val buffer = ByteBuffer.allocate(maxBytes)
    fn(buffer)
    apply(buffer, 0, maxBytes)
  }
  def apply(array : Array[Char]) : (Long, Long) = apply(array.size * 2, {_.asCharBuffer.put(array)})
  def apply(array : Array[Short]) : (Long, Long) = apply(array.size * 2, {_.asShortBuffer.put(array)})
  def apply(array : Array[Int]) : (Long, Long) = apply(array.size * 4, {_.asIntBuffer.put(array)})
  def apply(array : Array[Float]) : (Long, Long) = apply(array.size * 4, {_.asFloatBuffer.put(array)})
  def apply(array : Array[Long]) : (Long, Long) = apply(array.size * 8, {_.asLongBuffer.put(array)})
  def apply(array : Array[Double]) : (Long, Long) = apply(array.size * 8, {_.asDoubleBuffer.put(array)})

  def apply(value : Char) : (Long, Long)= apply(2, {_.asCharBuffer.put(value)})
  def apply(value : Short) : (Long, Long) = apply(2, {_.asShortBuffer.put(value)})
  def apply(value : Int) : (Long, Long) = apply(4, {_.asIntBuffer.put(value)})
  def apply(value : Float) : (Long, Long) = apply(4, {_.asFloatBuffer.put(value)})
  def apply(value : Long) : (Long, Long) = apply(8, {_.asLongBuffer.put(value)})
  def apply(value : Double) : (Long, Long) = apply(8, {_.asDoubleBuffer.put(value)})

  def apply(string : CharSequence) : (Long, Long) = apply(string.length * 2, {buffer =>
    val charBuffer = buffer.asCharBuffer
    0.to(string.length - 1).foreach{i => charBuffer.put(string.charAt(i))}
  })
}
