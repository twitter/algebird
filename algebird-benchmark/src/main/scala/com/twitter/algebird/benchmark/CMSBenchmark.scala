package com.twitter.algebird.benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import com.twitter.algebird.{ TopPctCMS, CMSHasherImplicits, TopPctCMSMonoid }

/**
 * Benchmarks the Count-Min sketch implementation in Algebird.
 *
 * We benchmark different `K` types as well as different input data streams.
 */

object CMSBenchmark {

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

    @Param(Array("100"))
    var operations: Int = 0 // Number of operations per benchmark repetition (cf. `reps`)

    @Param(Array("2048"))
    var maxBits: Int = 0

    var random: scala.util.Random = _
    var cmsLongMonoid: TopPctCMSMonoid[Long] = _
    var cmsBigIntMonoid: TopPctCMSMonoid[BigInt] = _
    var cmsStringMonoid: TopPctCMSMonoid[String] = _
    var inputsBigInt: Seq[BigInt] = _
    var inputsString: Seq[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      // Required import of implicit values (e.g. for BigInt- or Long-backed CMS instances)
      import CMSHasherImplicits._

      cmsLongMonoid = TopPctCMS.monoid[Long](eps, delta, Seed, heavyHittersPct)
      cmsBigIntMonoid = TopPctCMS.monoid[BigInt](eps, delta, Seed, heavyHittersPct)
      cmsStringMonoid = TopPctCMS.monoid[String](eps, delta, Seed, heavyHittersPct)

      random = new scala.util.Random

      inputsString = (0 to operations).map { i => random.nextString(maxBits / JavaCharSizeInBits) }.toSeq
      Console.out.println(s"Created ${inputsString.size} input records for String")
      inputsBigInt = inputsString.map { s => BigInt(s.getBytes) }
      Console.out.println(s"Created ${inputsBigInt.size} input records for BigInt")
    }
  }
}

class CMSBenchmark {
  import CMSBenchmark._
  // Case A (K=Long): We count the first hundred integers, i.e. [1, 100]
  @Benchmark
  def timePlusOfFirstHundredIntegersWithLongCms(state: CMSState) = {
    (1 to state.operations).view.foldLeft(state.cmsLongMonoid.zero)((l, r) => { l ++ state.cmsLongMonoid.create(r) })
  }

  // Case B.1 (K=BigInt): We count the first hundred integers, i.e. [1, 100]
  @Benchmark
  def timePlusOfFirstHundredIntegersWithBigIntCms(state: CMSState) = {
    (1 to state.operations).view.foldLeft(state.cmsBigIntMonoid.zero)((l, r) => { l ++ state.cmsBigIntMonoid.create(r) })
  }

  // Case B.2 (K=BigInt): We count numbers drawn randomly from a 2^maxBits address space
  @Benchmark
  def timePlusOfRandom2048BitNumbersWithBigIntCms(state: CMSState) = {
    state.inputsBigInt.view.foldLeft(state.cmsBigIntMonoid.zero)((l, r) => l ++ state.cmsBigIntMonoid.create(r))
  }

  // Case C (K=String): We count strings drawn randomly from a 2^maxBits address space
  @Benchmark
  def timePlusOfRandom2048BitNumbersWithStringCms(state: CMSState) = {
    state.inputsString.view.foldLeft(state.cmsStringMonoid.zero)((l, r) => l ++ state.cmsStringMonoid.create(r))
  }

}