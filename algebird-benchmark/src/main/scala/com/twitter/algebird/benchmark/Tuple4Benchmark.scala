package com.twitter.algebird
package benchmark

import scala.util.Random
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object Tuple4Benchmark {
  type Long4 = (Long, Long, Long, Long)
  @State(Scope.Benchmark)
  class Tuple4State {

    /**
     * This monoid lives in `GeneratedAbstractAlgebra.scala`.
     */
    val tupleMonoid: Monoid[Long4] = implicitly

    /**
     * This monoid lives in `GeneratedProductAlgebra.scala`.
     */
    val productMonoid: Monoid[Long4] =
      Monoid[Long4, Long, Long, Long, Long](Tuple4.apply, Tuple4.unapply)

    @Param(Array("10000"))
    var numElements: Int = 0

    var inputData: Seq[(Long, Long, Long, Long)] = _

    private def randL: Long = Random.nextInt(1000).toLong

    @Setup(Level.Trial)
    def setup(): Unit =
      inputData = Seq.fill(numElements)((randL, randL, randL, randL))
  }
}

class Tuple4Benchmark {
  import Tuple4Benchmark._

  @Benchmark
  def timeTuplePlus(state: Tuple4State, bh: Blackhole): Unit =
    bh.consume(state.inputData.reduce(state.tupleMonoid.plus(_, _)))

  @Benchmark
  def timeTupleSumOption(state: Tuple4State, bh: Blackhole): Unit =
    bh.consume(state.tupleMonoid.sumOption(state.inputData))

  @Benchmark
  def timeProductPlus(state: Tuple4State, bh: Blackhole): Unit =
    bh.consume(state.inputData.reduce(state.productMonoid.plus(_, _)))

  @Benchmark
  def timeProductSumOption(state: Tuple4State, bh: Blackhole): Unit =
    bh.consume(state.productMonoid.sumOption(state.inputData))
}
