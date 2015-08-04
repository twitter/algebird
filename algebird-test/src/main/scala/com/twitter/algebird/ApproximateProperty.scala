package com.twitter.algebird

import org.scalacheck.{ Gen, Prop }

trait ApproximateProperty {
  type Exact
  type Approx
  type Input
  type Result

  def exactGenerator: Gen[Exact]
  def inputGenerator(e: Exact): Gen[Input]

  def makeApproximate(e: Exact): Approx

  def exactResult(e: Exact, i: Input): Result
  def approximateResult(a: Approx, i: Input): Approximate[Result]
}

object ApproximateProperty {

  /**
   *  Generates a stream of exactly n Ts.
   *  Useful because `gen.apply` gives us Option[T], while we often want
   *  List[T].
   */
  def genListOf[T](gen: Gen[T], n: Int): Stream[T] =
    Stream.continually(()).map { _ => gen.apply(Gen.Parameters.default) }.flatten.take(n)

  def toProp(a: ApproximateProperty, objectReps: Int, inputReps: Int, falsePositiveRate: Double): Prop = {
    val successesAndProbabilities: Stream[(Int, Double)] = genListOf(a.exactGenerator, objectReps)
      .flatMap { exact =>
        val approx = a.makeApproximate(exact)
        genListOf(a.inputGenerator(exact), inputReps).map { input =>
          val approxResult = a.approximateResult(approx, input)
          val exactResult = a.exactResult(exact, input)
          val success = if (approxResult.boundsContain(exactResult)) 1 else 0
          (success, approxResult.probWithinBounds)
        }
      }
  }
}
