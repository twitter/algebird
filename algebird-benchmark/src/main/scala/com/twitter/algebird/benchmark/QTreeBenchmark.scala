package com.twitter.algebird.benchmark

import com.twitter.algebird._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random

object QTreeBenchmark {

  @State(Scope.Benchmark)
  class QTreeState {
    var qtreeUnit: QTreeSemigroup[Unit] = _

    var qtreeLong: QTreeSemigroup[Long] = _
    var qtreeDouble: QTreeSemigroup[Double] = _

    @Param(Array("5", "10", "12"))
    var depthK: Int = 0

    @Param(Array("100", "10000"))
    var numElements: Int = 0

    var inputDataUnit: Seq[QTree[Unit]] = _

    var inputDataLong: Seq[QTree[Long]] = _
    var inputDataDouble: Seq[QTree[Double]] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      qtreeUnit = new QTreeSemigroup[Unit](depthK)
      qtreeLong = new QTreeSemigroup[Long](depthK)
      qtreeDouble = new QTreeSemigroup[Double](depthK)

      inputDataUnit = {
        val rng = new Random("qtree".hashCode)

        (0L until numElements).map(_ => QTree((rng.nextInt(1000).toLong, ())))
      }

      inputDataLong = {
        val rng = new Random("qtree".hashCode)
        (0L until numElements).map(_ => QTree(rng.nextInt(1000).toLong))
      }

      inputDataDouble = {
        val rng = new Random("qtree".hashCode)

        (0L until numElements).map(_ => QTree(rng.nextInt(1000).toDouble))
      }

    }
  }
}

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class QTreeBenchmark {
  import QTreeBenchmark._
  @Benchmark
  def timeSumOptionUnit(state: QTreeState): Option[QTree[Unit]] =
    state.qtreeUnit.sumOption(state.inputDataUnit)

  @Benchmark
  def timeSumOptionLong(state: QTreeState): Option[QTree[Long]] =
    state.qtreeLong.sumOption(state.inputDataLong)

  @Benchmark
  def timeSumOptionDouble(state: QTreeState): Option[QTree[Double]] =
    state.qtreeDouble.sumOption(state.inputDataDouble)

  @Benchmark
  def timePlusUnit(state: QTreeState): QTree[Unit] =
    state.inputDataUnit.tail.reduce(state.qtreeUnit.plus)

  @Benchmark
  def timePlusLong(state: QTreeState): QTree[Long] =
    state.inputDataLong.tail.reduce(state.qtreeLong.plus)

  @Benchmark
  def timePlusDouble(state: QTreeState): QTree[Double] =
    state.inputDataDouble.tail.reduce(state.qtreeDouble.plus)

  @Benchmark
  def timeQuantileBounds(state: QTreeState): Seq[(Double, Double)] = {
    state.inputDataUnit.map(_.quantileBounds(0))
    state.inputDataUnit.map(_.quantileBounds(0.25))
    state.inputDataUnit.map(_.quantileBounds(0.5))
    state.inputDataUnit.map(_.quantileBounds(0.75))
    state.inputDataUnit.map(_.quantileBounds(1))
  }

}
