package com.twitter.algebird.benchmark

import org.openjdk.jmh.annotations._
import com.twitter.algebird.CMSHasher

/**
 * Benchmarks the hashing algorithms used by Count-Min sketch for CMS[BigInt].
 *
 * The input values are generated ahead of time to ensure that each trial uses the same input (and that the
 * RNG is not influencing the runtime of the trials).
 *
 * More details available at https://github.com/twitter/algebird/issues/392.
 */
// Once we can convince cappi (https://github.com/softprops/capp) -- the sbt plugin we use to run
// caliper benchmarks -- to work with the latest caliper 1.0-beta-1, we would:
//     - Let `CMSHashingBenchmark` extend `Benchmark` (instead of `SimpleBenchmark`)
//     - Annotate `timePlus` with `@MacroBenchmark`.
object CMSHashingBenchmark {
  @State(Scope.Benchmark)
  class CMSState {

    /**
     * The `a` parameter for CMS' default ("legacy") hashing algorithm: `h_i(x) = a_i * x + b_i (mod p)`.
     */
    @Param(Array("5123456"))
    var a: Int = 0

    /**
     * The `b` parameter for CMS' default ("legacy") hashing algorithm: `h_i(x) = a_i * x + b_i (mod p)`.
     *
     * Algebird's CMS implementation hard-codes `b` to `0`.
     */
    @Param(Array("0"))
    var b: Int = 0

    /**
     * Width of the counting table.
     */
    @Param(
      Array(
        "11" /* eps = 0.271 */,
        "544" /* eps = 0.005 */,
        "2719" /* eps = 1E-3 */,
        "271829" /* eps = 1E-5 */
      )
    )
    var width: Int = 0

    /**
     * Number of operations per benchmark repetition.
     */
    @Param(Array("100000"))
    var operations: Int = 0

    /**
     * Maximum number of bits for randomly generated BigInt instances.
     */
    @Param(Array("128", "1024", "2048"))
    var maxBits: Int = 0

    var random: scala.util.Random = _
    var inputs: Seq[BigInt] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      random = new scala.util.Random
      // We draw numbers randomly from a 2^maxBits address space.
      inputs = (1 to operations).view.map(_ => scala.math.BigInt(maxBits, random))
    }

  }
}
class CMSHashingBenchmark {
  import CMSHashingBenchmark._

  private def murmurHashScala(a: Int, b: Int, width: Int)(x: BigInt) = {
    val h = CMSHasher.hashBytes(a, b, width)(x.toByteArray)
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

  def timeBrokenCurrentHashWithRandomMaxBitsNumbers(state: CMSState): Unit =
    state.inputs.foreach(input => brokenCurrentHash(state.a, state.b, state.width)(input))

  def timeMurmurHashScalaWithRandomMaxBitsNumbers(state: CMSState): Unit =
    state.inputs.foreach(input => murmurHashScala(state.a, state.b, state.width)(input))

}
