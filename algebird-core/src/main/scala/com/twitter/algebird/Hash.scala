package com.twitter.algebird

import java.nio._

import com.twitter.bijection._

//TODO consider making a type alias for Injection[K, Hash32] and so on
object Hash {
  case class OneWayFunction[A, B](f: A => B) extends Injection[A, B] {
    override def apply(v: A): B = f(v)
    override def invert(v: B) = throw new UnsupportedOperationException("Cannot call invert on a one way injection")
  }
  object OneWayFunction {
    implicit def fromInjection[A, B](inj: Injection[A, B]): OneWayFunction[A, B] = OneWayFunction(inj.apply)
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

  case class MurmurByteBufferArgs(buffer: ByteBuffer, offset: Int, length: Int)
  object MurmurByteBufferArgs {
    implicit def fromBytes(bytes: Array[Byte]): MurmurByteBufferArgs =
      MurmurByteBufferArgs(ByteBuffer.wrap(bytes), 0, bytes.length)
  }

  def murmur128ByteBuffer(seed: Long): OneWayFunction[MurmurByteBufferArgs, Hash128] =
    Hash { case MurmurByteBufferArgs(buffer, offset, length) =>
      val longs = CassandraMurmurHash.hash3_x64_128(buffer, offset, length, seed)
      Hash128(longs(0), longs(1))
    }

  def murmur128(seed: Long): OneWayFunction[Array[Byte], Hash128] =
    OneWayFunction { bytes: Array[Byte] =>
      MurmurByteBufferArgs(ByteBuffer.wrap(bytes), 0, bytes.length)
    } andThen murmur128ByteBuffer(seed)

  val string2murmurargs: OneWayFunction[CharSequence, MurmurByteBufferArgs] =
    OneWayFunction { string =>
      val maxBytes = string.length * 2
      val buffer = ByteBuffer.allocate(maxBytes)
      val view = buffer.asCharBuffer
      0.to(string.length - 1).foreach { i => view.put(string.charAt(i)) }
      MurmurByteBufferArgs(buffer, 0, maxBytes)
    }

  val long2murmurargs: OneWayFunction[Long, MurmurByteBufferArgs] =
    OneWayFunction { long =>
      val buffer = ByteBuffer.allocate(8)
      buffer.asLongBuffer.put(long)
      MurmurByteBufferArgs(buffer, 0, 8)
    }
}

sealed trait Hash
case class Hash32(get: Int) extends Hash
case class Hash64(get: Long) extends Hash
case class Hash128(get: (Long, Long)) extends Hash
