package com.twitter.algebird

import java.nio._

case class MurmurHash128(seed: Long) extends AnyVal {
  def apply(buffer: ByteBuffer, offset: Int, length: Int): (Long, Long) = {
    val longs = CassandraMurmurHash.hash3_x64_128(buffer, offset, length, seed)
    (longs(0), longs(1))
  }

  def apply(bytes: Array[Byte]): (Long, Long) =
    apply(ByteBuffer.wrap(bytes), 0, bytes.length)
  def apply(maxBytes: Int, fn: ByteBuffer => Unit): (Long, Long) = {
    val buffer = ByteBuffer.allocate(maxBytes)
    fn(buffer)
    apply(buffer, 0, maxBytes)
  }
  def apply(array: Array[Char]): (Long, Long) =
    apply(array.size * 2, { _.asCharBuffer.put(array) })
  def apply(array: Array[Short]): (Long, Long) =
    apply(array.size * 2, { _.asShortBuffer.put(array) })
  def apply(array: Array[Int]): (Long, Long) =
    apply(array.size * 4, { _.asIntBuffer.put(array) })
  def apply(array: Array[Float]): (Long, Long) =
    apply(array.size * 4, { _.asFloatBuffer.put(array) })
  def apply(array: Array[Long]): (Long, Long) =
    apply(array.size * 8, { _.asLongBuffer.put(array) })
  def apply(array: Array[Double]): (Long, Long) =
    apply(array.size * 8, { _.asDoubleBuffer.put(array) })

  def apply(value: Char): (Long, Long) = apply(2, { _.asCharBuffer.put(value) })
  def apply(value: Short): (Long, Long) =
    apply(2, { _.asShortBuffer.put(value) })
  def apply(value: Int): (Long, Long) = apply(4, { _.asIntBuffer.put(value) })
  def apply(value: Float): (Long, Long) =
    apply(4, { _.asFloatBuffer.put(value) })
  def apply(value: Long): (Long, Long) = apply(8, { _.asLongBuffer.put(value) })
  def apply(value: Double): (Long, Long) =
    apply(8, { _.asDoubleBuffer.put(value) })

  def apply(string: CharSequence): (Long, Long) =
    apply(string.length * 2, { buffer =>
      val charBuffer = buffer.asCharBuffer
      0.to(string.length - 1).foreach { i =>
        charBuffer.put(string.charAt(i))
      }
    })
}
