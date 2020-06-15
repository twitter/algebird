package com.twitter.algebird

package benchmark

import org.openjdk.jmh.annotations._

import scala.util.Random

object BloomFilterCreateBenchmark {

  def createRandomString(nbrOfStrings: Int, lengthOfStrings: Int): Seq[String] =
    Seq.fill(nbrOfStrings)(Random.nextString(lengthOfStrings))

  @State(Scope.Benchmark)
  class BloomFilterState {
    @Param(Array("10000", "50000"))
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

  @Benchmark
  def createBloomFilterUsingFold(bloomFilterState: BloomFilterState): BF[String] = {
    val bfMonoid = BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bf = bloomFilterState.randomStrings.foldLeft(bfMonoid.zero) {
      case (filter, string) => filter + string
    }
    bf
  }

  @Benchmark
  def createBloomFilterUsingFoldExperimental(
      bloomFilterState: BloomFilterState
  ): experimental.BloomFilter[String]#BF =
    bloomFilterState.randomStrings.foldLeft(bloomFilterState.experimentalBF.zero) {
      case (filter, string) => filter + string
    }

  @Benchmark
  def createBloomFilterAggregator(bloomFilterState: BloomFilterState): BF[String] = {
    val bfMonoid = BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bfAggregator = BloomFilterAggregator(bfMonoid)
    val bf = bloomFilterState.randomStrings.aggregate(bfAggregator.monoid.zero)(_ + _, _ ++ _)
    bf
  }

  @Benchmark
  def createBloomFilterAggregatorExperimental(
      bloomFilterState: BloomFilterState
  ) =
    bloomFilterState.randomStrings
      .aggregate(bloomFilterState.experimentalBF.aggregator.monoid.zero)(_ + _, (a, b) => a ++ b)
}
