package com.twitter.algebird.caliper

import com.twitter.algebird._
import scala.util.Random
import com.twitter.bijection._

import java.util.concurrent.Executors
import com.twitter.algebird.util._
import com.google.caliper.{ Param, SimpleBenchmark }
import java.nio.ByteBuffer

import scala.math._

class OldQTreeSemigroup[A: Monoid](k: Int) extends QTreeSemigroup[A](k) {
  override def sumOption(items: TraversableOnce[QTree[A]]) =
    if (items.isEmpty) None
    else Some(items.reduce(plus))
}

class QTreeBenchmark extends SimpleBenchmark {
  var qtree: QTreeSemigroup[Long] = _
  var oldqtree: QTreeSemigroup[Long] = _

  @Param(Array("5", "10", "12"))
  val depthK: Int = 0

  @Param(Array("100", "10000"))
  val numElements: Int = 0

  var inputData: Seq[QTree[Long]] = _

  override def setUp {
    qtree = new QTreeSemigroup[Long](depthK)
    oldqtree = new OldQTreeSemigroup(depthK)

    val rng = new Random("qtree".hashCode)

    inputData = (0L until numElements).map { _ =>
      QTree(rng.nextInt(1000).toLong)
    }
  }

  def timeSumOption(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      qtree.sumOption(inputData)
      dummy += 1
    }
    dummy
  }

  /*
  def timeOldSumOption(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      oldqtree.sumOption(inputData)
      dummy += 1
    }
    dummy
  } */
}
