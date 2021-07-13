package com.twitter.algebird.benchmark

import com.twitter.algebird.mutable.{BloomFilter, BloomFilterAggregator, MutableBF}
import com.twitter.algebird.benchmark.BloomFilterCreateBenchmark.createRandomString
import org.openjdk.jmh.annotations._

object MutableBloomFilterCreateBenchmark {

  @State(Scope.Benchmark)
  class BloomFilterState {
    @Param(Array("100", "1000", "10000"))
    var nbrOfElements: Int = 0

    @Param(Array("0.001", "0.01"))
    var falsePositiveRate: Double = 0

    var randomStrings: Seq[String] = _

    @Setup(Level.Trial)
    def setup(): Unit =
      randomStrings = createRandomString(nbrOfElements, 10)

  }
}
class MutableBloomFilterBenchmark {

  import MutableBloomFilterCreateBenchmark._

  @Benchmark
  def createMutableBloomFilter(bloomFilterState: BloomFilterState): MutableBF[String] = {
    val bfMonoid = BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bf = bfMonoid.create(bloomFilterState.randomStrings: _*)
    bf
  }

  @Benchmark
  def createMutableBloomFilterUsingFold(bloomFilterState: BloomFilterState): MutableBF[String] = {
    val bfMonoid = BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bf = bloomFilterState.randomStrings.foldLeft(bfMonoid.zero) {
      case (filter, string) => filter += string
    }
    bf
  }

  @Benchmark
  def createMutableBloomFilterAggregator(bloomFilterState: BloomFilterState): MutableBF[String] = {
    val bfMonoid = BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bfAggregator = BloomFilterAggregator(bfMonoid)

    val bf = bloomFilterState.randomStrings.aggregate(bfAggregator.monoid.zero)(_ += _, _ ++= _)
    bf
  }

}
