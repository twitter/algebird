package com.twitter.algebird
package benchmark

import org.openjdk.jmh.annotations._

object MutableBloomFilterQueryBenchmark {

  @State(Scope.Benchmark)
  class BloomFilterState {

    @Param(Array("100", "1000", "10000"))
    var nbrOfElements: Int = 0

    @Param(Array("0.001", "0.01"))
    var falsePositiveRate: Double = 0

    var bf: mutable.MutableBF[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val randomStrings =
        BloomFilterCreateBenchmark.createRandomString(nbrOfElements, 10)
      bf = mutable
        .BloomFilter[String](nbrOfElements, falsePositiveRate)
        .create(randomStrings: _*)
    }
  }
}

class MutableBloomFilterQueryBenchmark {
  import MutableBloomFilterQueryBenchmark._

  @Benchmark
  def queryBloomFilter(bloomFilterState: BloomFilterState): ApproximateBoolean =
    bloomFilterState.bf.contains("1")
}
