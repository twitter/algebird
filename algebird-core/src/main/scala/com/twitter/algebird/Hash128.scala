/*
Copyright 2015 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.twitter.algebird

/**
 * A typeclass to represent hashing to 128 bits.
 * Used for HLL, but possibly other applications
 */
trait Hash128[-K] extends java.io.Serializable {

  def DefaultSeed: Long

  def hashWithSeed(seed: Long, k: K): (Long, Long)

  def hash(k: K): (Long, Long) = hashWithSeed(DefaultSeed, k)

  def contramap[L](fn: L => K): Hash128[L] = {
    val self = this
    new Hash128[L] {
      override val DefaultSeed: Long = self.DefaultSeed
      override def hashWithSeed(seed: Long, l: L): (Long, Long) = self.hashWithSeed(seed, fn(l))
    }
  }
}

/**
 * This gives default hashes using Murmur128 with a seed
 * of 12345678 (for no good reason, but it should not be changed
 * lest we break serialized HLLs)
 */
object Hash128 extends java.io.Serializable {
  def hash[K](k: K)(implicit h: Hash128[K]): (Long, Long) = h.hash(k)

  val DefaultSeed: Long = 12345678L

  def murmur128ArrayByte(seed: Long): Hash128[Array[Byte]] =
    new Hash128[Array[Byte]] {
      override val DefaultSeed: Long = seed
      override def hashWithSeed(seed: Long, k: Array[Byte]): (Long, Long) = MurmurHash128(seed)(k)
    }
  def murmur128ArrayInt(seed: Long): Hash128[Array[Int]] =
    new Hash128[Array[Int]] {
      override val DefaultSeed: Long = seed
      override def hashWithSeed(seed: Long, k: Array[Int]): (Long, Long) = MurmurHash128(seed)(k)
    }
  def murmur128ArrayLong(seed: Long): Hash128[Array[Long]] =
    new Hash128[Array[Long]] {
      override val DefaultSeed: Long = seed
      override def hashWithSeed(seed: Long, k: Array[Long]): (Long, Long) = MurmurHash128(seed)(k)
    }

  /**
   * This serializes the string into UTF-8, then hashes. This is different
   * than the UTF-16 based approach in Murmur128.apply(CharSequence),
   * but has been more commonly used in HLL.
   */
  def murmur128Utf8String(defaultSeed: Long): Hash128[String] =
    murmur128ArrayByte(defaultSeed).contramap(_.getBytes("UTF-8"))

  def murmur128Int(defaultSeed: Long): Hash128[Int] = new Hash128[Int] {
    override val DefaultSeed: Long = defaultSeed
    override def hashWithSeed(seed: Long, k: Int): (Long, Long) = MurmurHash128(seed)(k)
  }
  def murmur128Long(defaultSeed: Long): Hash128[Long] = new Hash128[Long] {
    override val DefaultSeed: Long = defaultSeed
    override def hashWithSeed(seed: Long, k: Long): (Long, Long) = MurmurHash128(seed)(k)
  }

  implicit lazy val arrayByteHash: Hash128[Array[Byte]] = murmur128ArrayByte(DefaultSeed)
  implicit lazy val arrayIntHash: Hash128[Array[Int]] = murmur128ArrayInt(DefaultSeed)
  implicit lazy val arrayLongHash: Hash128[Array[Long]] = murmur128ArrayLong(DefaultSeed)
  implicit lazy val stringHash: Hash128[String] = murmur128Utf8String(DefaultSeed)
  implicit lazy val intHash: Hash128[Int] = murmur128Int(DefaultSeed)
  implicit lazy val longHash: Hash128[Long] = murmur128Long(DefaultSeed)
}
