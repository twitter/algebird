package com.twitter.algebird
package benchmark

import com.twitter.algebird.legacy._
import org.openjdk.jmh.annotations._

import scala.util.Random

object BloomFilterLegacyCreateBenchmark {

  def createRandomString(nbrOfStrings: Int, lengthOfStrings: Int): Seq[String] =
    Seq.fill(nbrOfStrings)(Random.nextString(lengthOfStrings))

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

class BloomFilterLegacyCreateBenchmark {

  import BloomFilterLegacyCreateBenchmark._

  @Benchmark
  def createBloomFilter(bloomFilterState: BloomFilterState): BF[String] = {
    val bfMonoid = BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bf = bfMonoid.create(bloomFilterState.randomStrings: _*)
    bf
  }
}
