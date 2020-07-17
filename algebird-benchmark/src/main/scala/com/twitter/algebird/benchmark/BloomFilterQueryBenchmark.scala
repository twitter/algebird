package com.twitter.algebird

package benchmark

import org.openjdk.jmh.annotations._

object BloomFilterQueryBenchmark {

  @State(Scope.Benchmark)
  class BloomFilterState {

    @Param(Array("100", "1000", "10000"))
    var nbrOfElements: Int = 0

    @Param(Array("0.001", "0.01"))
    var falsePositiveRate: Double = 0

    var bf: BF[String] = _
    var experimentalBF: experimental.BloomFilter[String]#Hash = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val randomStrings =
        BloomFilterCreateBenchmark.createRandomString(nbrOfElements, 10)
      bf = BloomFilter[String](nbrOfElements, falsePositiveRate)
        .create(randomStrings: _*)
      experimentalBF = experimental
        .BloomFilter[String](nbrOfElements, falsePositiveRate)
        .create(randomStrings: _*)
    }
  }
}

class BloomFilterQueryBenchmark {
  import BloomFilterQueryBenchmark._

  @Benchmark
  def queryBloomFilter(bloomFilterState: BloomFilterState): ApproximateBoolean =
    bloomFilterState.bf.contains("1")

  @Benchmark
  def queryBloomFilterExperimental(bloomFilterState: BloomFilterState): ApproximateBoolean =
    bloomFilterState.experimentalBF.contains("1")
}
