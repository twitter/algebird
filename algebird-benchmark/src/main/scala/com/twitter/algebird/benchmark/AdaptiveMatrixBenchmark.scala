package com.twitter.algebird.benchmark

import com.twitter.algebird._
import com.twitter.bijection._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random
import com.twitter.algebird.matrix._

object AdaptiveMatrixBenchmark {

  @State(Scope.Benchmark)
  class AdaptiveMatrixState {

    // error = 5%, 1% and 0.5%
    @Param(Array("5", "10", "20"))
    var rows: Int = 0

    @Param(Array("50", "200", "500"))
    var cols: Int = 0

    @Param(Array("100", "10000"))
    var numElements: Int = 0

    var inputData: Seq[AdaptiveMatrix[Long]] = _

    val monoid = implicitly[Monoid[AdaptiveMatrix[Long]]]

    @Setup(Level.Trial)
    def setup(): Unit = {
      inputData = {
        val rng = new Random("adaptiveMatrix".hashCode)

        (0L until numElements).map { _ =>
          AdaptiveMatrix.fill(rows, cols)(rng.nextLong)
        }

      }

    }
  }
}

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class AdaptiveMatrixBenchmark {
  import AdaptiveMatrixBenchmark._
  @Benchmark
  def timeSumOption(state: AdaptiveMatrixState) = {
    state.monoid.sumOption(state.inputData)
  }

  @Benchmark
  def timePlus(state: AdaptiveMatrixState): AdaptiveMatrix[Long] = {
    state.inputData.reduce(state.monoid.plus)
  }

}
