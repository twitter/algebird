package com.twitter.algebird.benchmark

import com.twitter.algebird._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random

object QTreeMicroBenchmark {

  @State(Scope.Benchmark)
  class QTreeMicroState {
    @Param(Array("0", "1", "3", "5"))
    var extendToLevel: Int = 0

    @Param(Array("100", "10000"))
    var numElements: Int = 0

    var inputDataLong: Seq[QTree[Long]] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val rng = new Random("qtree".hashCode)

      inputDataLong = (0L until numElements).map { _ =>
        QTree(rng.nextInt(1000).toLong)
      }

    }
  }

  @annotation.tailrec
  private final def extendToLevelDoubleBranch[A](q: QTree[A], n: Int)(implicit monoid: Monoid[A]): QTree[A] =
    if (n <= q.level)
      q
    else {
      val nextLevel = q.level + 1
      val nextOffset = q.offset / 2

      val l = if (q.offset % 2 == 0) q else null
      val r = if (q.offset % 2 == 0) null else q

      val parent =
        new QTree[A](monoid.zero, nextOffset, nextLevel, q.count, l, r)

      extendToLevelDoubleBranch(parent, n)
    }

  @annotation.tailrec
  private final def extendToLevelSingleBranch[A](q: QTree[A], n: Int)(implicit monoid: Monoid[A]): QTree[A] =
    if (n <= q.level)
      q
    else {
      val nextLevel = q.level + 1
      val nextOffset = q.offset / 2

      val parent =
        if (q.offset % 2 == 0)
          new QTree[A](monoid.zero, nextOffset, nextLevel, q.count, q, null)
        else
          new QTree[A](monoid.zero, nextOffset, nextLevel, q.count, null, q)

      extendToLevelSingleBranch(parent, n)
    }
}

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class QTreeMicroBenchmark {
  import QTreeMicroBenchmark._

  @Benchmark
  def doubleBranch(state: QTreeMicroState) = {
    val iter = state.inputDataLong.toIterator
    while (iter.hasNext) {
      extendToLevelDoubleBranch(iter.next, state.extendToLevel)
    }
  }

  @Benchmark
  def singleBranch(state: QTreeMicroState) = {
    val iter = state.inputDataLong.toIterator
    while (iter.hasNext) {
      extendToLevelSingleBranch(iter.next, state.extendToLevel)
    }
  }

}
