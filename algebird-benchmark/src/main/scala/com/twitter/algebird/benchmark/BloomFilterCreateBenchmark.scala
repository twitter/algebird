package com.twitter.algebird
package benchmark

import org.openjdk.jmh.annotations._

import scala.util.Random

object BloomFilterCreateBenchmark {

  def createRandomString(nbrOfStrings: Int, lengthOfStrings: Int): Seq[String] = {
    Seq.fill(nbrOfStrings)(Random.nextString(lengthOfStrings))
  }

  implicit def stringToBytes(s: String): Array[Byte] = s.getBytes()

  @State(Scope.Benchmark)
  class BloomFilterState {
    @Param(Array("4", "5", "6"))
    var nbrOfHashes: Int = 0

    @Param(Array("16", "32", "64"))
    var width: Int = 0

    val randomString = createRandomString(1000, 10)
  }
}

class BloomFilterCreateBenchmark {

  import BloomFilterCreateBenchmark._

  @Benchmark
  def createBloomFilter(bloomFilterState: BloomFilterState): BF[String] = {
    val bfMonoid = new BloomFilterMonoid[String](bloomFilterState.nbrOfHashes, bloomFilterState.width)
    val bf = bfMonoid.create(bloomFilterState.randomString: _*)
    bf
  }
}

