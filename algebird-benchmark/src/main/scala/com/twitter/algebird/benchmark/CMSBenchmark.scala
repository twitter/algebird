package com.twitter.algebird
package benchmark

import org.openjdk.jmh.annotations._
import scala.util.Random.nextString

/**
 * Benchmarks the Count-Min sketch (CMS) implementation in Algebird.
 *
 * We benchmark different `K` types as well as different input data streams.
 */
object CMSBenchmark {
  import CMSHasherImplicits.CMSHasherBigInt

  @State(Scope.Benchmark)
  class CMSState {

    val Seed: Int = 1
    val MaxBits: Int = 2048

    @Param(Array("0.1", "0.005"))
    var eps: Double = 0.0

    @Param(Array("0.0000001")) // 1e-8
    var delta: Double = 0.0

    // number of data values to combine into a CMS
    @Param(Array("1000"))
    var size: Int = 0

    // need to initialize later because we don't have `size` yet.
    var smallLongs: Vector[Long] = _
    var smallBigInts: Vector[BigInt] = _
    var largeBigInts: Vector[BigInt] = _
    var smallBigDecimals: Vector[BigDecimal] = _
    var largeBigDecimals: Vector[BigDecimal] = _
    var largeStrings: Vector[String] = _

    // need to initialize later because we don't have `eps` and `delta` yet.
    var longMonoid: CMSMonoid[Long] = _
    var bigIntMonoid: CMSMonoid[BigInt] = _
    var bigDecimalMonoid: CMSMonoid[BigDecimal] = _
    var stringMonoid: CMSMonoid[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      longMonoid = CMS.monoid[Long](eps, delta, Seed)
      bigIntMonoid = CMS.monoid[BigInt](eps, delta, Seed)
      bigDecimalMonoid = CMS.monoid[BigDecimal](eps, delta, Seed)
      stringMonoid = CMS.monoid[String](eps, delta, Seed)

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

  def sumCmsVector[A](as: Vector[A], m: CMSMonoid[A]): CMS[A] =
    m.sum(as.iterator.map(CMSItem(_, 1L, m.params)))
}

class CMSBenchmark {
  import CMSBenchmark._

  @Benchmark
  def sumSmallLongCms(st: CMSState): CMS[Long] =
    sumCmsVector(st.smallLongs, st.longMonoid)

  @Benchmark
  def sumSmallBigIntCms(st: CMSState): CMS[BigInt] =
    sumCmsVector(st.smallBigInts, st.bigIntMonoid)

  @Benchmark
  def sumLargeBigIntCms(st: CMSState): CMS[BigInt] =
    sumCmsVector(st.largeBigInts, st.bigIntMonoid)

  @Benchmark
  def sumSmallBigDecimalCms(st: CMSState): CMS[BigDecimal] =
    sumCmsVector(st.smallBigDecimals, st.bigDecimalMonoid)

  @Benchmark
  def sumLargeBigDecimalCms(st: CMSState): CMS[BigDecimal] =
    sumCmsVector(st.largeBigDecimals, st.bigDecimalMonoid)

  @Benchmark
  def sumLargeStringCms(st: CMSState): CMS[String] =
    sumCmsVector(st.largeStrings, st.stringMonoid)
}
