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
  def hash(k: K): (Long, Long)
  def contramap[L](fn: L => K): Hash128[L] = {
    val self = this
    new Hash128[L] {
      def hash(l: L) = self.hash(fn(l))
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

  val DefaultSeed = 12345678L

  def murmur128ArrayByte(seed: Long): Hash128[Array[Byte]] = new Hash128[Array[Byte]] {
    def hash(k: Array[Byte]) = MurmurHash128(seed)(k)
  }
  def murmur128ArrayInt(seed: Long): Hash128[Array[Int]] = new Hash128[Array[Int]] {
    def hash(k: Array[Int]) = MurmurHash128(seed)(k)
  }
  def murmur128ArrayLong(seed: Long): Hash128[Array[Long]] = new Hash128[Array[Long]] {
    def hash(k: Array[Long]) = MurmurHash128(seed)(k)
  }

  /**
   * This serializes the string into UTF-8, then hashes. This is different
   * than the UTF-16 based approach in Murmur128.apply(CharSequence),
   * but has been more commonly used in HLL.
   */
  def murmur128Utf8String(seed: Long): Hash128[String] =
    murmur128ArrayByte(seed).contramap(_.getBytes("UTF-8"))

  def murmur128Int(seed: Long): Hash128[Int] = new Hash128[Int] {
    def hash(k: Int) = MurmurHash128(seed)(k)
  }
  def murmur128Long(seed: Long): Hash128[Long] = new Hash128[Long] {
    def hash(k: Long) = MurmurHash128(seed)(k)
  }

  implicit lazy val arrayByteHash: Hash128[Array[Byte]] = murmur128ArrayByte(DefaultSeed)
  implicit lazy val arrayIntHash: Hash128[Array[Int]] = murmur128ArrayInt(DefaultSeed)
  implicit lazy val arrayLongHash: Hash128[Array[Long]] = murmur128ArrayLong(DefaultSeed)
  implicit lazy val stringHash: Hash128[String] = murmur128Utf8String(DefaultSeed)
  implicit lazy val intHash: Hash128[Int] = murmur128Int(DefaultSeed)
  implicit lazy val longHash: Hash128[Long] = murmur128Long(DefaultSeed)
}
