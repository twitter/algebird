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

  private def successesAndProbabilities(a: ApproximateProperty, objectReps: Int, inputReps: Int): (Int, Double) = {
    val stream: Stream[(Int, Double)] = genListOf(a.exactGenerator, objectReps)
      .flatMap { exact =>
        val approx = a.makeApproximate(exact)
        genListOf(a.inputGenerator(exact), inputReps).map { input =>
          val approxResult = a.approximateResult(approx, input)
          val exactResult = a.exactResult(exact, input)
          val success = if (approxResult.boundsContain(exactResult)) 1 else 0
          (success, approxResult.probWithinBounds)
        }
      }
    val monoid = implicitly[Monoid[(Int, Double)]]
    monoid.sum(stream)
  }

  def toProp(a: ApproximateProperty, objectReps: Int, inputReps: Int, falsePositiveRate: Double): Prop = {
    require(0 <= falsePositiveRate && falsePositiveRate <= 1)

    val (successes, sumOfProbabilities) = successesAndProbabilities(a, objectReps, inputReps)
    val n = objectReps * inputReps
    (sumOfProbabilities - successes) > scala.math.sqrt(n * scala.math.log(falsePositiveRate) / -2)
  }

  def toProp(a: Iterable[ApproximateProperty], objectReps: Int, inputReps: Int, falsePositiveRate: Double): Prop = {
    require(0 <= falsePositiveRate && falsePositiveRate <= 1)

    val monoid = implicitly[Monoid[(Int, Double)]]
    val (successes, sumOfProbabilities) = monoid.sum(a.map { approximateProp =>
      successesAndProbabilities(approximateProp, objectReps, inputReps)
    })
    val n = objectReps * inputReps

    println("Foo")
    println(sumOfProbabilities - successes)

    (sumOfProbabilities - successes) > scala.math.sqrt(n * scala.math.log(falsePositiveRate) / -2)
  }
}
