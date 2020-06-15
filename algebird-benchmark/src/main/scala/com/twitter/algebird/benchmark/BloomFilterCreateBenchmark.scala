package com.twitter.algebird

package benchmark

import org.openjdk.jmh.annotations._

import scala.util.Random

object BloomFilterCreateBenchmark {

  def createRandomString(nbrOfStrings: Int, lengthOfStrings: Int): Seq[String] =
    Seq.fill(nbrOfStrings)(Random.nextString(lengthOfStrings))

  @State(Scope.Benchmark)
  class BloomFilterState {
    @Param(Array("100", "1000", "10000"))
    var nbrOfElements: Int = 0

    @Param(Array("0.001", "0.01"))
    var falsePositiveRate: Double = 0

    var randomStrings: Seq[String] = _
    var experimentalBF: experimental.BloomFilter[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      randomStrings = createRandomString(nbrOfElements, 10)
      experimentalBF = experimental.BloomFilter[String](nbrOfElements, falsePositiveRate)
    }

  }
}

class BloomFilterCreateBenchmark {

  import BloomFilterCreateBenchmark._

  @Benchmark
  def createBloomFilter(bloomFilterState: BloomFilterState): BF[String] = {
    val bfMonoid = BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bf = bfMonoid.create(bloomFilterState.randomStrings: _*)
    bf
  }

  @Benchmark
  def createBloomFilterExperimental(bloomFilterState: BloomFilterState): experimental.BloomFilter[String]#BF =
    bloomFilterState.experimentalBF.create(bloomFilterState.randomStrings: _*)
}
