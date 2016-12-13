package com.twitter.algebird
package benchmark

import org.openjdk.jmh.annotations._

object BloomFilterQueryBenchmark {

  import BloomFilterCreateBenchmark.stringToBytes

  @State(Scope.Benchmark)
  class BloomFilterState {
    @Param(Array("4", "5", "6"))
    var nbrOfHashes: Int = 0

    @Param(Array("16", "32", "64"))
    var width: Int = 0
    val bfMonoid = new BloomFilterMonoid[String](6, 32)
    val randomStrings = BloomFilterCreateBenchmark.createRandomString(1000, 10)
    val bf = bfMonoid.create(randomStrings: _*)
  }
}

class BloomFilterQueryBenchmark {
  import BloomFilterQueryBenchmark._

  @Benchmark
  def queryBloomFilter(bloomFilterState: BloomFilterState): ApproximateBoolean = {
    bloomFilterState.bf.contains("1")
  }
}
