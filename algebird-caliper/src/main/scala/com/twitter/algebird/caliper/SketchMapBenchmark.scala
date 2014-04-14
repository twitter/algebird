package com.twitter.algebird.caliper

import com.twitter.algebird._
import scala.util.Random
import com.twitter.bijection._

import java.util.concurrent.Executors
import com.twitter.algebird.util._
import com.google.caliper.{Param, Benchmark}
import com.twitter.algebird.HyperLogLogMonoid
import java.nio.ByteBuffer

import scala.math._

class SketchMapBenchmark extends Benchmark {
  type SkMap = SketchMap[String, Long]
  implicit val byteEncoder = implicitly[Injection[String, Array[Byte]]].toFunction
  var sketchMapMonoid: SketchMapMonoid[String, Long] = _
  var params: SketchMapParams[String] = _

  @Param(Array("4000", "8000"))
  val width: Int = 0

  @Param(Array("2", "4", "8"))
  val depth: Int = 0

  @Param(Array("100", "200", "500"))
  val heavy_hitters_count: Int = 0

  @Param(Array("10", "20", "100"))
  val innerKeyMaxInit: Int = 0

  @Param(Array("100", "200"))
  val innerKeyMaxPoolSize: Int = 0

  @Param(Array("1", "10"))
  val numInputKeys: Int = 0

  @Param(Array("10", "1000", "10000"))
  val numElements: Int = 0

  val SEED = 1

  var inputData: Seq[Seq[SkMap]] = _

  override def setUp {
    val rng = new Random(3)
    params = SketchMapParams[String](SEED, width, depth, heavy_hitters_count)

    sketchMapMonoid = SketchMap.monoid[String, Long](params)

    def innerKeyCount = rng.nextInt(innerKeyMaxInit) + 1

    val innerKeys = (0 until innerKeyMaxPoolSize).map {_ =>
      rng.nextString(10)
    }.toIndexedSeq

    val inputIntermediate = (0L until numElements).map {_ =>
      val curCount: Int = innerKeyCount
      val tups = (0 until curCount).map { _ =>
        val indx = rng.nextInt(innerKeys.size)
        (innerKeys(indx), rng.nextInt(1000).toLong)
      }.toList
      (pow(numInputKeys, rng.nextFloat).toLong, List(sketchMapMonoid.create(tups)))
    }
    inputData = MapAlgebra.sumByKey(inputIntermediate).map(_._2).toSeq
  }

  def timeSumOption(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      inputData.foreach(sketchMapMonoid.sumOption(_))
      dummy += 1
    }
    dummy
  }

  // def timePlus(reps: Int): Int = {
  //   var dummy = 0
  //   while (dummy < reps) {
  //     inputData.foreach { vals =>
  //       vals.reduce(sketchMapMonoid.plus(_, _))
  //     }
  //     dummy += 1
  //   }
  //   dummy
  // }
}
