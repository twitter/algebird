package com.twitter.algebird.benchmark

import com.twitter.algebird._
import com.twitter.bijection._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random

class OldQTreeSemigroup[A: Monoid](k: Int) extends QTreeSemigroup[A](k) {
  override def sumOption(items: TraversableOnce[QTree[A]]) =
    if (items.isEmpty) None
    else Some(items.reduce(plus))
}

object QTreeBenchmark {

  @State(Scope.Benchmark)
  class QTreeState {
    var qtree: QTreeSemigroup[Long] = _
    var oldqtree: QTreeSemigroup[Long] = _

    @Param(Array("5", "10", "12"))
    var depthK: Int = 0

    @Param(Array("100", "10000"))
    var numElements: Int = 0

    var inputData: Seq[QTree[Long]] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      qtree = new QTreeSemigroup[Long](depthK)
      oldqtree = new OldQTreeSemigroup(depthK)

      val rng = new Random("qtree".hashCode)

      inputData = (0L until numElements).map { _ =>
        QTree(rng.nextInt(1000).toLong)
      }
    }
  }
}

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class QTreeBenchmark {
  import QTreeBenchmark._

  @Benchmark
  def timeSumOption(state: QTreeState) = {
    state.qtree.sumOption(state.inputData)
  }

  @Benchmark
  def timeOldSumOption(state: QTreeState) = {
    state.oldqtree.sumOption(state.inputData)
  }
}
