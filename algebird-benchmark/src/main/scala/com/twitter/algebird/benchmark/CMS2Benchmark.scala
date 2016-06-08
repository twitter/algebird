package com.twitter.algebird
package benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

/**
 * Benchmarks the Count-Min sketch implementation in Algebird.
 *
 * We benchmark different `K` types as well as different input data streams.
 */

object CMS2Benchmark {

  @State(Scope.Benchmark)
  class CMSState {

    val Seed = 1
    val JavaCharSizeInBits = 2 * 8

    @Param(Array("0.1", "0.005"))
    var eps: Double = 0.0

    @Param(Array("0.0000001" /* 1E-8 */ ))
    var delta: Double = 0.0

    @Param(Array("1000"))
    var ops: Int = 0 // Number of operations per benchmark repetition (cf. `reps`)

    @Param(Array("2048"))
    var maxBits: Int = 0

    var random: scala.util.Random = _

    var cmsLongSemigroup: Semigroup[CMS2[Long]] = _
    var cmsBigIntSemigroup: Semigroup[CMS2[BigInt]] = _
    var cmsStringSemigroup: Semigroup[CMS2[String]] = _

    var inputsBigInt: Vector[BigInt] = _
    var inputsString: Vector[String] = _

    val BatchSize = 1000

    var batchedLongSemigroup: Semigroup[Batched[CMS2[Long]]] = _
    var batchedBigIntSemigroup: Semigroup[Batched[CMS2[BigInt]]] = _
    var batchedStringSemigroup: Semigroup[Batched[CMS2[String]]] = _

    @Setup(Level.Trial)
    def setup(): Unit = {

      // Required import of implicit values (e.g. for BigInt- or Long-backed CMS instances)
      import CMSHasherImplicits._

      implicit val longContext: CMS2.Context[Long] = CMS2.Context(delta, eps, Seed)
      implicit val bigIntContext: CMS2.Context[BigInt] = CMS2.Context(delta, eps, Seed)
      implicit val stringContext: CMS2.Context[String] = CMS2.Context(delta, eps, Seed)

      cmsLongSemigroup = CMS2.cms2Monoid[Long]
      cmsBigIntSemigroup = CMS2.cms2Monoid[BigInt]
      cmsStringSemigroup = CMS2.cms2Monoid[String]

      batchedLongSemigroup = new BatchedSemigroup(BatchSize, cmsLongSemigroup)
      batchedBigIntSemigroup = new BatchedSemigroup(BatchSize, cmsBigIntSemigroup)
      batchedStringSemigroup = new BatchedSemigroup(BatchSize, cmsStringSemigroup)

      random = new scala.util.Random

      inputsString = (0 to ops).map(i => random.nextString(maxBits / JavaCharSizeInBits)).toVector
      Console.out.println(s"Created ${inputsString.size} input records for String")
      inputsBigInt = inputsString.map(s => BigInt(s.getBytes)).toVector
      Console.out.println(s"Created ${inputsBigInt.size} input records for BigInt")
    }
  }
}

class CMS2Benchmark {

  import CMS2Benchmark._

  // Case A (K=Long): We count the integers [1, N].
  @Benchmark
  def timePlusOfFirstHundredIntegersWithLongCms2(st: CMSState) =
    st.cmsLongSemigroup.sumOption((1 to st.ops).iterator.map(n => CMS2(n.toLong)))
  // st.batchedLongSemigroup.sumOption((1 to st.ops).iterator.map(n => Batched(CMS2(n.toLong))))
  //   .map(_.sum(st.cmsLongSemigroup))
  //(1 to st.ops).iterator.map(n => CMS2(n.toLong)).reduceLeft(st.cmsLongSemigroup.plus)

  // Case B.1 (K=BigInt): We count the integers [1, N].
  @Benchmark
  def timePlusOfFirstHundredIntegersWithBigIntCms2(st: CMSState) =
    st.cmsBigIntSemigroup.sumOption((1 to st.ops).iterator.map(n => CMS2(BigInt(n))))
  // st.batchedBigIntSemigroup.sumOption((1 to st.ops).iterator.map(n => Batched(CMS2(BigInt(n)))))
  //   .map(_.sum(st.cmsBigIntSemigroup))
  //(1 to st.ops).iterator.map(n => CMS2(BigInt(n))).reduceLeft(st.cmsBigIntSemigroup.plus)

  // Case B.2 (K=BigInt): We count numbers drawn randomly from a 2^maxBits address space
  @Benchmark
  def timePlusOfRandom2048BitNumbersWithBigIntCms2(st: CMSState) =
    st.cmsBigIntSemigroup.sumOption(st.inputsBigInt.iterator.map(n => CMS2(n)))
  // st.batchedBigIntSemigroup.sumOption(st.inputsBigInt.iterator.map(n => Batched(CMS2(n))))
  //   .map(_.sum(st.cmsBigIntSemigroup))
  //st.inputsBigInt.iterator.map(n => CMS2(n)).reduceLeft(st.cmsBigIntSemigroup.plus)

  // Case C (K=String): We count strings drawn randomly from a 2^maxBits address space
  @Benchmark
  def timePlusOfRandom2048BitNumbersWithStringCms2(st: CMSState) =
    st.cmsStringSemigroup.sumOption(st.inputsString.iterator.map(s => CMS2(s)))
  // st.batchedStringSemigroup.sumOption(st.inputsString.iterator.map(s => Batched(CMS2(s))))
  //   .map(_.sum(st.cmsStringSemigroup))
  //st.inputsString.iterator.map(s => CMS2(s)).reduceLeft(st.cmsStringSemigroup.plus)
}
