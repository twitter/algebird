package com.twitter.algebird

import java.nio._

import com.twitter.bijection.Injection

//TODO consider making a type alias for Injection[K, Hash32] and so on
object Hash {
  case class OneWayFunction[A, B](f: A => B) extends Injection[A, B] {
    override def apply(v: A): B = f(a)
    override def invert(v: B): Try[A] =
      throw new UnsupportedOperationException("Cannot call invert on a one way injection")
  }

  private val PRIME_MODULUS = (1L << 31) - 1

  def apply[A, B <: Hash](f: A => B): OneWayFunction[A, B] = OneWayFunction(f)

  val hash64to32: OneWayFunction[Hash64, Hash32] = Hash { case Hash64(v) => Hash32(((v >>> 32) ^ v).toInt) }
  val hash128to64: OneWayFunction[Hash128, Hash64] = Hash { case Hash128((l, r)) => Hash64(l ^ r) }
  val hash128to32: OneWayFunction[Hash128, Hash32] = hash128to64 andThen hash64to32

  /**
   * This is a generally useful hash that facilitates creating a number of hashes that can
   * be used together. The Count-Min sketch, for example, uses pairwise independent hash
   * functions drawn from a universal hashing family of the form
   *
   *   h(x) = [a * x + b (mod p)] (mod width)
   */
  def universal(a: Int, b: Int, width: Int): OneWayFunction[Long, Hash32] = Hash { x =>
    val unmodded = a * x + b

    // Apparently a super fast way of computing x mod 2^p-1
    // See page 149 of
    // http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // after Proposition 7.
    val modded = (unmodded + (unmodded >> 32)) & PRIME_MODULUS

    // Modulo-ing integers is apparently twice as fast as
    // modulo-ing Longs.
    Hash32(modded.toInt % width)
  }

  def murmur128(seed: Long): OneWayFunction[Array[Byte], Hash128] = {
    val murmur = MurmurHash128(seed)
    Hash { bytes => Hash128(murmur(bytes)) }
  }
}

sealed trait Hash
case class Hash32(v: Int)
case class Hash64(v: Long)
case class Hash128(v: (Long, Long))

//TODO do we just get rid of this?
case class MurmurHash128(seed :  Long) {
  def apply(buffer : ByteBuffer, offset : Int, length : Int) : (Long,Long) = {
    val longs = CassandraMurmurHash.hash3_x64_128(buffer, offset, length, seed)
    (longs(0), longs(1))
  }

  def apply(bytes : Array[Byte]) : (Long, Long) = apply(ByteBuffer.wrap(bytes), 0, bytes.length)
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
