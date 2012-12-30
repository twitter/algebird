package com.twitter.algebird

import java.nio.ByteBuffer
import com.twitter.hashing.Hashable

object MurmurHashable128 {
  val defaultSeed = 1234567891234567891L
  private def generateBuffer(bytes: Array[Byte]): ByteBuffer = {
    val buf = ByteBuffer.wrap(bytes)
    buf.position(bytes.size)
    buf
  }
  private def generateBuffer(maxBytes: Int)(fn: ByteBuffer => Unit): ByteBuffer = {
    val buffer = ByteBuffer.allocate(maxBytes)
    fn(buffer)
    buffer
  }
  def apply[T](seed: Long)(fn: T => ByteBuffer) =
    new MurmurHashable128[T](seed) {
      override def getByteBuffer(t: T) = fn(t)
    }
  def onByteArray(seed: Long = defaultSeed): MurmurHashable128[Array[Byte]] =
    apply(seed) { generateBuffer(_) }
  def onCharArray(seed: Long = defaultSeed): MurmurHashable128[Array[Char]] =
    apply(seed) { arr =>
      generateBuffer(arr.size * 2) { _.asCharBuffer.put(arr) }
    }
  def onShortArray(seed: Long = defaultSeed): MurmurHashable128[Array[Short]] =
    apply(seed) { arr =>
      generateBuffer(arr.size * 2) { _.asShortBuffer.put(arr) }
    }
  def onIntArray(seed: Long = defaultSeed): MurmurHashable128[Array[Int]] =
    apply(seed) { arr =>
      generateBuffer(arr.size * 4) { _.asIntBuffer.put(arr) }
    }
  def onFloatArray(seed: Long = defaultSeed): MurmurHashable128[Array[Float]] =
    apply(seed) { arr =>
      generateBuffer(arr.size * 4) { _.asFloatBuffer.put(arr) }
    }
  def onLongArray(seed: Long = defaultSeed): MurmurHashable128[Array[Long]] =
    apply(seed) { arr =>
      generateBuffer(arr.size * 8) { _.asLongBuffer.put(arr) }
    }
  def onDoubleArray(seed: Long = defaultSeed): MurmurHashable128[Array[Double]] =
    apply(seed) { arr =>
      generateBuffer(arr.size * 8) { _.asDoubleBuffer.put(arr) }
    }
  def onChar(seed: Long = defaultSeed): MurmurHashable128[Char] =
    apply(seed) { value => generateBuffer(2) { _.asCharBuffer.put(value) } }
  def onShort(seed: Long = defaultSeed): MurmurHashable128[Short] =
    apply(seed) { value => generateBuffer(2) { _.asShortBuffer.put(value) } }
  def onInt(seed: Long = defaultSeed): MurmurHashable128[Int] =
    apply(seed) { value => generateBuffer(4) { _.asIntBuffer.put(value) } }
  def onFloat(seed: Long = defaultSeed): MurmurHashable128[Float] =
    apply(seed) { value => generateBuffer(4) { _.asFloatBuffer.put(value) } }
  def onLong(seed: Long = defaultSeed): MurmurHashable128[Long] =
    apply(seed) { value => generateBuffer(8) { _.asLongBuffer.put(value) } }
  def onDouble(seed: Long = defaultSeed): MurmurHashable128[Double] =
    apply(seed) { value => generateBuffer(8) { _.asDoubleBuffer.put(value) } }
  def onString(seed: Long = defaultSeed): MurmurHashable128[String] = apply(seed) { s => generateBuffer(s.getBytes) }
}

abstract class MurmurHashable128[T](seed: Long) extends Hashable[T, (Long, Long)] {
  // The returned ByteBuffer needs to store all data from idx 0 to
  // "position".
  def getByteBuffer(t: T): ByteBuffer

  override def apply(t: T): (Long, Long) = {
    val buf = getByteBuffer(t)
    val longs = CassandraMurmurHash.hash3_x64_128(buf, 0, buf.position, seed)
    (longs(0), longs(1))
  }
  def withSeed(newSeed: Long) = MurmurHashable128(newSeed) { t: T => this.getByteBuffer(t) }
}

// TODO: Once util-bijection is published, define each of these using
// a Bijection[T,Array[Byte]].

case class MurmurHash128(seed: Long) {
  def apply(bytes: Array[Byte]): (Long, Long) = MurmurHashable128.onByteArray(seed)(bytes)
  def apply(array: Array[Char]): (Long, Long) = MurmurHashable128.onCharArray(seed)(array)
  def apply(array: Array[Short]): (Long, Long) = MurmurHashable128.onShortArray(seed)(array)
  def apply(array: Array[Int]): (Long, Long) = MurmurHashable128.onIntArray(seed)(array)
  def apply(array: Array[Float]): (Long, Long) = MurmurHashable128.onFloatArray(seed)(array)
  def apply(array: Array[Long]): (Long, Long) = MurmurHashable128.onLongArray(seed)(array)
  def apply(array: Array[Double]): (Long, Long) = MurmurHashable128.onDoubleArray(seed)(array)

  def apply(value: Char): (Long, Long) = MurmurHashable128.onChar(seed)(value)
  def apply(value: Short): (Long, Long) = MurmurHashable128.onShort(seed)(value)
  def apply(value: Int): (Long, Long) = MurmurHashable128.onInt(seed)(value)
  def apply(value: Float): (Long, Long) = MurmurHashable128.onFloat(seed)(value)
  def apply(value: Long): (Long, Long) = MurmurHashable128.onLong(seed)(value)
  def apply(value: Double): (Long, Long) = MurmurHashable128.onDouble(seed)(value)
  def apply(string: String): (Long, Long) = MurmurHashable128.onString(seed)(string)
}
