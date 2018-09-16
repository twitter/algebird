package com.twitter.algebird
package benchmark

import org.openjdk.jmh.annotations._

import scala.util.Random.nextString

/**
 * [info] Benchmark                                (bucket)  (depth)  (size)   Mode  Cnt       Score       Error  Units
 * [info] AMSJoinBenchmark.amsJoinBenchmarkString        18       10    1000  thrpt    3  148071,642 ± 98359,117  ops/s
 * [info] AMSJoinBenchmark.amsJoinBenchmarkString        18      200    1000  thrpt    3    5926,345 ±   332,592  ops/s
 *
 * */
object CMSJoinBenchmark {

  @State(Scope.Benchmark)
  class AMSJoinState {

    val Seed: Int = 1
    val MaxBits: Int = 2048

    @Param(Array("0.1", "0.005"))
    var eps: Double = 0.0

    @Param(Array("0.0000001")) // 1e-8
    var delta: Double = 0.0

    // number of data values to combine into a CMS
    @Param(Array("1000"))
    var size: Int = 0

    var stringMonoid: CMSMonoid[String] = _

    var cmsSketch1: CMS[String] = _
    var cmsSketch2: CMS[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val largeStringsSample1 = (1 to size).map(i => nextString(10)).toVector
      val largeStringsSample2 = (1 to size).map(i => nextString(10)).toVector

      stringMonoid = CMS.monoid[String](eps, delta, Seed)

      cmsSketch1 = stringMonoid.create(largeStringsSample1)
      cmsSketch2 = stringMonoid.create(largeStringsSample2)
    }
  }
}
class CMSJoinBenchmark {
  import CMSJoinBenchmark._

  @Benchmark
  def amsJoinBenchmarkString(amsS: AMSJoinState): Unit =
    amsS.cmsSketch1.innerProduct(amsS.cmsSketch2)
}
