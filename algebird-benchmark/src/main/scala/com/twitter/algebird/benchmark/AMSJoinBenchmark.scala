package com.twitter.algebird
package benchmark
import org.openjdk.jmh.annotations._

import scala.util.Random.nextString

/**
 * [info] Benchmark                                (bucket)  (depth)  (size)   Mode  Cnt      Score       Error  Units
 * [info] AMSJoinBenchmark.amsJoinBenchmarkString        27       16    1000  thrpt    3  55539,549 ± 44815,541  ops/s
 * [info] AMSJoinBenchmark.amsJoinBenchmarkString       543       16    1000  thrpt    3   3036,712 ±  2487,763  ops/s
 *
 * [info] Benchmark                                  (delta)  (eps)  (size)   Mode  Cnt      Score      Error  Units
 * [info] CMSJoinBenchmark.amsJoinBenchmarkString  0.0000001    0.1    1000  thrpt    3  37416,918 ± 4578,109  ops/s
 * [info] CMSJoinBenchmark.amsJoinBenchmarkString  0.0000001  0.005    1000  thrpt    3   3194,029 ±  335,681  ops/s
 */
object AMSJoinBenchmark {

  @State(Scope.Benchmark)
  class AMSJoinState {

    @Param(Array("27", "543"))
    var bucket = 0

    @Param(Array("16"))
    var depth = 0

    @Param(Array("1000"))
    var size: Int = 0

    var amsMonoidString: AMSMonoid[String] = _

    var amsSketch1: AMS[String] = _
    var amsSketch2: AMS[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val largeStringsSample1 = (1 to size).map(i => nextString(10)).toVector
      val largeStringsSample2 = (1 to size).map(i => nextString(10)).toVector

      amsMonoidString = new AMSMonoid[String](depth, bucket)
      amsSketch1 = amsMonoidString.create(largeStringsSample1)
      amsSketch2 = amsMonoidString.create(largeStringsSample2)
    }
  }
}
class AMSJoinBenchmark {
  import AMSJoinBenchmark._

  @Benchmark
  def amsJoinBenchmarkString(amsS: AMSJoinState): Unit =
    amsS.amsSketch1.innerProduct(amsS.amsSketch2)
}
