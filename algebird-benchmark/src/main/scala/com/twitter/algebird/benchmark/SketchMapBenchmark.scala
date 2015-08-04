package com.twitter.algebird.benchmark

import com.twitter.algebird._
import com.twitter.bijection._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random

object SketchMapBenchmark {

  @State(Scope.Benchmark)
  class SketchMapState {
    val seed: Int = 1618

    // error = 5%, 1% and 0.5%
    @Param(Array("0.05", "0.01", "0.005"))
    var epsilon: Double = 0

    @Param(Array("0.007"))
    var delta: Double = -0

    @Param(Array("1", "10", "100"))
    var heavyHittersCount: Int = 10

    var skmMonoid: SketchMapMonoid[String, Long] = _

    @Param(Array("100", "10000"))
    var numElements: Int = 0

    @Param(Array("1", "10", "100"))
    var density: Int = 0

    @Param(Array("1", "10", "20"))
    var stringLength: Int = 0

    var inputData: Seq[SketchMap[String, Long]] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      skmMonoid = new SketchMapMonoid(SketchMapParams(seed, epsilon, delta, heavyHittersCount)(Injection.utf8.apply))

      inputData = {
        val rng = new Random("sketchmap".hashCode)

        (0L until numElements).map { _ =>
          val seq = (0 until (rng.nextInt(density) + 1)).map { _ =>
            val genString = rng.nextString(rng.nextInt(stringLength))
            genString -> 1L
          }
          skmMonoid.create(seq)
        }
      }
    }
  }
}

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class SketchMapBenchmark {
  import SketchMapBenchmark._
  @Benchmark
  def timeSumOption(state: SketchMapState) = {
    state.skmMonoid.sumOption(state.inputData)
  }

  @Benchmark
  def timePlus(state: SketchMapState): SketchMap[String, Long] = {
    state.inputData.reduce(state.skmMonoid.plus)
  }

}
