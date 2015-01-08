package com.twitter.algebird.caliper

import com.google.caliper.{Param, SimpleBenchmark}

/**
 * Benchmarks the hashing algorithms used by Count-Min sketch for CMS[BigInt].
 *
 * The input values are generated ahead of time to ensure that each trial uses the same input (and that the RNG is not
 * influencing the runtime of the trials).
 *
 * More details available at https://github.com/twitter/algebird/issues/392.
 */
// Once we can convince cappi (https://github.com/softprops/capp) -- the sbt plugin we use to run
// caliper benchmarks -- to work with the latest caliper 1.0-beta-1, we would:
//     - Let `CMSHashingBenchmark` extend `Benchmark` (instead of `SimpleBenchmark`)
//     - Annotate `timePlus` with `@MacroBenchmark`.
class CMSHashingBenchmark extends SimpleBenchmark {

  /**
   * The `a` parameter for CMS' default ("legacy") hashing algorithm: `h_i(x) = a_i * x + b_i (mod p)`.
   */
  @Param(Array("5123456"))
  val a: Int = 0

  /**
   * The `b` parameter for CMS' default ("legacy") hashing algorithm: `h_i(x) = a_i * x + b_i (mod p)`.
   *
   * Algebird's CMS implementation hard-codes `b` to `0`.
   */
  @Param(Array("0"))
  val b: Int = 0

  /**
   * Width of the counting table.
   */
  @Param(Array("11" /* eps = 0.271 */ , "544" /* eps = 0.005 */ , "2719" /* eps = 1E-3 */ , "271829" /* eps = 1E-5 */))
  val width: Int = 0

  /**
   * Number of operations per benchmark repetition.
   */
  @Param(Array("100000"))
  val operations: Int = 0

  /**
   * Maximum number of bits for randomly generated BigInt instances.
   */
  @Param(Array("128", "1024", "2048"))
  val maxBits: Int = 0

  var random: scala.util.Random = _
  var inputs: Seq[BigInt] = _

  override def setUp() {
    random = new scala.util.Random
    // We draw numbers randomly from a 2^maxBits address space.
    inputs = (1 to operations).view.map { _ => scala.math.BigInt(maxBits, random)}
  }

  private def murmurHashScala(a: Int, b: Int, width: Int)(x: BigInt) = {
    val hash: Int = scala.util.hashing.MurmurHash3.arrayHash(x.toByteArray, a)
    val h = {
      // We only want positive integers for the subsequent modulo.  This method mimics Java's Hashtable implementation,
      // and it requires `hash` to be an `Int` = have 32 bits (to match with `0x7FFFFFFF`).
      val positiveHash = hash & 0x7FFFFFFF
      positiveHash % width
    }
    assert(h >= 0, "hash must not be negative")
    h
  }

  private val PRIME_MODULUS = (1L << 31) - 1

  private def brokenCurrentHash(a: Int, b: Int, width: Int)(x: BigInt) = {
    val unModded: BigInt = (x * a) + b
    val modded: BigInt = (unModded + (unModded >> 32)) & PRIME_MODULUS
    val h = modded.toInt % width
    assert(h >= 0, "hash must not be negative")
    h
  }

  def timeBrokenCurrentHashWithRandomMaxBitsNumbers(operations: Int): Int = {
    var dummy = 0
    while (dummy < operations) {
      inputs.foreach { input => brokenCurrentHash(a, b, width)(input)}
      dummy += 1
    }
    dummy
  }

  def timeMurmurHashScalaWithRandomMaxBitsNumbers(operations: Int): Int = {
    var dummy = 0
    while (dummy < operations) {
      inputs.foreach { input => murmurHashScala(a, b, width)(input)}
      dummy += 1
    }
    dummy
  }

}