package com.twitter.algebird
package benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random.nextString

/**
 * Benchmarks the Count-Min sketch implementation in Algebird.
 *
 * We benchmark different `K` types as well as different input data streams.
 */
object TopCMSBenchmark {
  import CMSHasherImplicits.CMSHasherBigInt

  @State(Scope.Benchmark)
  class CMSState {

    val Seed: Int = 1
    val MaxBits: Int = 2048

    @Param(Array("0.1", "0.005"))
    var eps: Double = 0.0

    @Param(Array("0.0000001")) // 1e-8
    var delta: Double = 0.0

    @Param(Array("0.2"))
    var pct: Double = 0.0

    @Param(Array("1000"))
    var size: Int = 0 // Number of operations per benchmark repetition (cf. `reps`)

    // need to initialize later because we don't have `size` yet.
    var smallLongs: Vector[Long] = _
    var smallBigInts: Vector[BigInt] = _
    var largeBigInts: Vector[BigInt] = _
    var smallBigDecimals: Vector[BigDecimal] = _
    var largeBigDecimals: Vector[BigDecimal] = _
    var largeStrings: Vector[String] = _

    var cmsLongMonoid: TopPctCMSMonoid[Long] = _
    var cmsBigIntMonoid: TopPctCMSMonoid[BigInt] = _
    var cmsBigDecimalMonoid: TopPctCMSMonoid[BigDecimal] = _
    var cmsStringMonoid: TopPctCMSMonoid[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      cmsLongMonoid = TopPctCMS.monoid[Long](eps, delta, Seed, pct)
      cmsBigIntMonoid = TopPctCMS.monoid[BigInt](eps, delta, Seed, pct)
      cmsBigDecimalMonoid = TopPctCMS.monoid[BigDecimal](eps, delta, Seed, pct)
      cmsStringMonoid = TopPctCMS.monoid[String](eps, delta, Seed, pct)

      val bitsPerChar = 16
      largeStrings = (1 to size).map(i => nextString(MaxBits / bitsPerChar)).toVector
      largeBigInts = largeStrings.map(s => BigInt(s.getBytes))
      largeBigDecimals = largeStrings.map(s => {
        val md = (s.head % 256) - 128
        BigDecimal(BigInt(s.tail.getBytes)) * BigDecimal(1).pow(md)
      })

      smallLongs = (1 to size).map(_.toLong).toVector
      smallBigInts = (1 to size).map(BigInt(_)).toVector
      smallBigDecimals = (1 to size).map(BigDecimal(_) + BigDecimal(1).pow(-size)).toVector
    }
  }

  def sumTopCmsVector[A](as: Vector[A], m: TopPctCMSMonoid[A]): TopCMS[A] =
    m.sum(as.iterator.map(m.create))
}

class TopCMSBenchmark {
  import TopCMSBenchmark._

  @Benchmark
  def sumSmallLongTopCms(st: CMSState) =
    sumTopCmsVector(st.smallLongs, st.cmsLongMonoid)

  @Benchmark
  def sumSmallBigIntTopCms(st: CMSState) =
    sumTopCmsVector(st.smallBigInts, st.cmsBigIntMonoid)

  @Benchmark
  def sumLargeBigIntTopCms(st: CMSState) =
    sumTopCmsVector(st.largeBigInts, st.cmsBigIntMonoid)

  @Benchmark
  def sumSmallBigDecimalTopCms(st: CMSState) =
    sumTopCmsVector(st.smallBigDecimals, st.cmsBigDecimalMonoid)

  @Benchmark
  def sumLargeBigDecimalTopCms(st: CMSState) =
    sumTopCmsVector(st.largeBigDecimals, st.cmsBigDecimalMonoid)

  @Benchmark
  def sumLargeStringTopCms(st: CMSState) =
    sumTopCmsVector(st.largeStrings, st.cmsStringMonoid)
}
