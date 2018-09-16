package com.twitter.algebird
package benchmark
import org.openjdk.jmh.annotations._

import scala.util.Random.nextString

/**
  * AMS is interesting to compute inner join between two of them.
  *
  * */
object AMSJoinBenchmark {

  @State(Scope.Benchmark)
  class AMSJoinState {

    @Param(Array("27", "543", "5438"))
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
