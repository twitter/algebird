package com.twitter.algebird.benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import com.twitter.algebird.{ TopPctCMS, CMSHasherImplicits, TopPctCMSMonoid }

/**
 * Benchmarks the Count-Min sketch implementation in Algebird.
 *
 * We benchmark different `K` types as well as different input data streams.
 */

object TopCMSBenchmark {

  @State(Scope.Benchmark)
  class CMSState {

    val Seed = 1
    val JavaCharSizeInBits = 2 * 8

    @Param(Array("0.1", "0.005"))
    var eps: Double = 0.0

    @Param(Array("0.0000001" /* 1E-8 */ ))
    var delta: Double = 0.0

    @Param(Array("0.2"))
    var heavyHittersPct: Double = 0.0

    @Param(Array("1000"))
    var ops: Int = 0 // Number of operations per benchmark repetition (cf. `reps`)

    @Param(Array("2048"))
    var maxBits: Int = 0

    var random: scala.util.Random = _
    var cmsLongMonoid: TopPctCMSMonoid[Long] = _
    var cmsBigIntMonoid: TopPctCMSMonoid[BigInt] = _
    var cmsStringMonoid: TopPctCMSMonoid[String] = _
    var inputsBigInt: Vector[BigInt] = _
    var inputsString: Vector[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      // Required import of implicit values (e.g. for BigInt- or Long-backed CMS instances)
      import CMSHasherImplicits._

      cmsLongMonoid = TopPctCMS.monoid[Long](eps, delta, Seed, heavyHittersPct)
      cmsBigIntMonoid = TopPctCMS.monoid[BigInt](eps, delta, Seed, heavyHittersPct)
      cmsStringMonoid = TopPctCMS.monoid[String](eps, delta, Seed, heavyHittersPct)

      random = new scala.util.Random

      inputsString = (0 to ops).map { i => random.nextString(maxBits / JavaCharSizeInBits) }.toVector
      Console.out.println(s"Created ${inputsString.size} input records for String")
      inputsBigInt = inputsString.map { s => BigInt(s.getBytes) }.toVector
      Console.out.println(s"Created ${inputsBigInt.size} input records for BigInt")
    }
  }
}

class TopCMSBenchmark {
  import TopCMSBenchmark._
  // Case A (K=Long): We count the first hundred integers, i.e. [1, 100]
  @Benchmark
  def timePlusOfFirstHundredIntegersWithLongCms(st: CMSState) = {
    val m = st.cmsLongMonoid
    m.sumOption((1 to st.ops).iterator.map(n => m.create(n)))
  }

  // Case B.1 (K=BigInt): We count the first hundred integers, i.e. [1, 100]
  @Benchmark
  def timePlusOfFirstHundredIntegersWithBigIntCms(st: CMSState) = {
    val m = st.cmsBigIntMonoid
    m.sumOption((1 to st.ops).iterator.map(n => m.create(BigInt(n))))
  }

  // Case B.2 (K=BigInt): We count numbers drawn randomly from a 2^maxBits address space
  @Benchmark
  def timePlusOfRandom2048BitNumbersWithBigIntCms(st: CMSState) = {
    val m = st.cmsBigIntMonoid
    m.sumOption(st.inputsBigInt.iterator.map(m.create(_)))
  }

  // Case C (K=String): We count strings drawn randomly from a 2^maxBits address space
  @Benchmark
  def timePlusOfRandom2048BitNumbersWithStringCms(st: CMSState) = {
    val m = st.cmsStringMonoid
    m.sumOption(st.inputsString.iterator.map(m.create(_)))
  }
}
