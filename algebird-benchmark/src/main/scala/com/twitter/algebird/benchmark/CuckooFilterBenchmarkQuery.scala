package com.twitter.algebird
package benchmark

import com.twitter.algebird.benchmark.CuckooFilterBenchmarkQuery.CuckooFilterState
import org.openjdk.jmh.annotations._

object CuckooFilterBenchmarkQuery {

  @State(Scope.Benchmark)
  class CuckooFilterState {

    @Param(Array("100", "1000", "10000"))
    var nbrOfElements: Int = 0

    @Param(Array("100", "300"))
    var bucketNumber: Int = 0

    @Param(Array("10", "50"))
    var fingerprintBucket: Int = 0

    var cf: CF[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val randomStrings =
        CuckooFilterCreateBenchmark.createRandomString(nbrOfElements, 10)
      cf = CuckooFilter[String](fingerprintBucket, bucketNumber)
        .create(randomStrings: _*)
    }
  }
}

class CuckooFilterBenchmarkQuery {

  @Benchmark
  def queryCuckooFilter(cuckooFilterState: CuckooFilterState): Boolean =
    cuckooFilterState.cf.lookup("1")
}
