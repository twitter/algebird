package com.twitter.algebird
package benchmark

import com.twitter.algebird.benchmark.CuckooFilterCreateBenchmark.CuckooFilterState
import org.openjdk.jmh.annotations._

import scala.util.Random

object CuckooFilterCreateBenchmark {
  def createRandomString(nbString: Int, sizeStr: Int): Seq[String] =
    Seq.fill(nbString)(Random.nextString(sizeStr))

  @State(Scope.Benchmark)
  class CuckooFilterState() {

    @Param(Array("100", "1000", "10000"))
    var nbrOfElements: Int = 0

    @Param(Array("100", "300"))
    var bucketNumber: Int = 0

    @Param(Array("10", "50"))
    var fingerprintBucket: Int = 0

    var randomStrings: Seq[String] = _

    @Setup(Level.Trial)
    def setup(): Unit =
      randomStrings = createRandomString(nbrOfElements, 10)
  }

}

class CuckooFilterCreateBenchmark {
  @Benchmark
  def createCuckooFilter(cuckooFilterState: CuckooFilterState): CF[String] = {
    val cfMonoid = CuckooFilter[String](cuckooFilterState.fingerprintBucket, cuckooFilterState.bucketNumber)
    val cf = cfMonoid.create(cuckooFilterState.randomStrings: _*)
    cf
  }
}
