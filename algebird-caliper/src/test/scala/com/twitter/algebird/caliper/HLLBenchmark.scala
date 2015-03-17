package com.twitter.algebird.caliper

import com.twitter.algebird._
import scala.util.Random
import com.twitter.bijection._

import java.util.concurrent.Executors
import com.twitter.algebird.util._
import com.google.caliper.{ Param, SimpleBenchmark }
import com.twitter.algebird.{ HyperLogLogMonoid, ReadOnlyBoxedArrayByte }
import java.nio.ByteBuffer

import scala.math._
class OldMonoid(bits: Int) extends HyperLogLogMonoid(bits) {
  import HyperLogLog._

  override def sumOption(items: TraversableOnce[HLL]): Option[HLL] =
    if (items.isEmpty) None
    else {
      val buffer = new Array[Byte](size)
      items.foreach { _.updateInto(buffer) }
      Some(DenseHLL(bits, ReadOnlyBoxedArrayByte(buffer)))
    }
}

class HllBenchmark extends SimpleBenchmark {
  var hllMonoid: HyperLogLogMonoid = _
  var oldHllMonoid: HyperLogLogMonoid = _

  @Param(Array("12", "14", "24"))
  val numBits: Int = 0

  // Old sum option will not work with >=100 keys, and >= 1000 elements.
  @Param(Array("1", "10"))
  val numInputKeys: Int = 0

  @Param(Array("10", "1000", "10000"))
  val numElements: Int = 0

  var inputData: Seq[Seq[HLL]] = _

  override def setUp {
    hllMonoid = new HyperLogLogMonoid(numBits)

    oldHllMonoid = new OldMonoid(numBits)

    val rng = new Random(3)

    val byteEncoder = implicitly[Injection[Long, Array[Byte]]]
    def setSize = rng.nextInt(10) + 1 // 1 -> 10
    def hll(elements: Set[Long]): HLL = hllMonoid.batchCreate(elements)(byteEncoder)

    val inputIntermediate = (0L until numElements).map { _ =>
      val setElements = (0 until setSize).map{ _ => rng.nextInt(1000).toLong }.toSet
      (pow(numInputKeys, rng.nextFloat).toLong, List(hll(setElements)))
    }
    inputData = MapAlgebra.sumByKey(inputIntermediate).map(_._2).toSeq
  }

  def timeSumOption(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      inputData.foreach(hllMonoid.sumOption(_))
      dummy += 1
    }
    dummy
  }

  def timeOldSumOption(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      inputData.foreach(oldHllMonoid.sumOption(_))
      dummy += 1
    }
    dummy
  }

  def timePlus(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      inputData.foreach { vals =>
        vals.reduce(hllMonoid.plus(_, _))
      }
      dummy += 1
    }
    dummy
  }
}
