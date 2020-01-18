package com.twitter.algebird
package benchmark

import com.twitter.algebird.legacy._
import org.openjdk.jmh.annotations._

object BloomFilterLegacyQueryBenchmark {

  @State(Scope.Benchmark)
  class BloomFilterState {

    @Param(Array("100", "1000", "10000"))
    var nbrOfElements: Int = 0

    @Param(Array("0.001", "0.01"))
    var falsePositiveRate: Double = 0

    var bf: BF[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val randomStrings =
        BloomFilterCreateBenchmark.createRandomString(nbrOfElements, 10)
      bf = BloomFilter[String](nbrOfElements, falsePositiveRate)
        .create(randomStrings: _*)
    }
  }
}

class BloomFilterLegacyQueryBenchmark {
  import BloomFilterLegacyQueryBenchmark._

  @Benchmark
  def queryBloomFilter(bloomFilterState: BloomFilterState): ApproximateBoolean =
    bloomFilterState.bf.contains("1")
}
