package com.twitter.algebird

import org.scalacheck.{Gen, Prop, Properties, Test}
import org.scalacheck.util.Pretty

trait ApproximateProperty {
  type Exact
  type Approx
  type Input
  type Result

  def exactGenerator: Gen[Exact]
  def inputGenerator(e: Exact): Gen[Input]

  def makeApproximate(e: Exact): Approx

  def exactResult(e: Exact, i: Input): Result
  def approximateResult(a: Approx, i: Input): ApproximateSet[Result]
}

object ApproximateProperty {

  /**
   * Generates a list of exactly n Ts. Useful because `Gen.listOfN(n, gen).sample` gives us Option[List[T]],
   * while we often want List[T].
   */
  @annotation.tailrec
  private def genListOf[T](n: Int, gen: Gen[T], trial: Int = 100): List[T] =
    Gen.listOfN(n, gen).sample match {
      case Some(xs) => xs
      case _        => if (trial <= 0) Nil else genListOf(n, gen, trial - 1)
    }

  private def successesAndProbabilities(
      a: ApproximateProperty,
      objectReps: Int,
      inputReps: Int
  ): List[(Int, Double, List[String])] =
    genListOf(objectReps, a.exactGenerator)
      .flatMap { exact =>
        val approx = a.makeApproximate(exact)
        genListOf(inputReps, a.inputGenerator(exact)).flatMap { input =>
          val approxResult = a.approximateResult(approx, input)
          val exactResult = a.exactResult(exact, input)

          val success = approxResult.contains(exactResult)
          if (success.withProb == 0.0) {
            None
          } else {
            val successInt = if (success.isTrue) 1 else 0
            val messages =
              if (success.isTrue) List()
              else
                List(s"Exact result: $exactResult. Approx result: $approxResult.")
            Some((successInt, success.withProb, messages))
          }
        }
      }

  def toProp(a: ApproximateProperty, objectReps: Int, inputReps: Int, falsePositiveRate: Double): Prop =
    Prop { _: Gen.Parameters =>
      require(0 <= falsePositiveRate && falsePositiveRate <= 1)

      val list = successesAndProbabilities(a, objectReps, inputReps)
      val n = list.length

      val monoid = implicitly[Monoid[(Int, Double, List[String])]]
      val (successes, sumOfProbabilities, exacts) = monoid.sum(list)

      // Computed from Hoeffding's inequality, might be inaccurate
      // TODO Make sure this is correct
      val diff = scala.math.sqrt(-n * scala.math.log(falsePositiveRate) / 2.0)

      val success =
        if (successes >= (sumOfProbabilities - diff)) Prop.Proof else Prop.False

      // Args that get printed when Scalacheck runs the test
      val argsList: List[(String, String)] = {
        val results = List(
          ("Successes", s"$successes (out of $n)"),
          ("Expected successes", "%.2f".format(sumOfProbabilities)),
          ("Required successes", "%.2f".format(sumOfProbabilities - diff))
        )

        val exampleFailures =
          if (success == Prop.False)
            List(("Example failures:\n  >", exacts.take(5).mkString("\n  >")))
          else List()

        val zeroProbTests = objectReps * inputReps - n
        val testsReturnedZeroProb =
          if (zeroProbTests > 0) {
            List(
              (
                "Omitted results",
                s"$zeroProbTests/${objectReps * inputReps} tests returned an Approximate with probability 0. These tests have been omitted from the calculation."
              )
            )
          } else List()

        results ++ exampleFailures ++ testsReturnedZeroProb
      }

      val args = argsList.map { case (name, value) =>
        Prop.Arg(name, value, 0, value, Pretty.prettyAny(value), Pretty.prettyAny(value))
      }

      Prop.Result(success, args = args)
    }

  /**
   * Converts a list of ApproximateProperties to a scalacheck Prop that fails if too many of the
   * ApproximateProperties fail. TODO use `new Prop` like the above `toProp` method so that we can have useful
   * error messages.
   */
  def toProp(
      a: Seq[ApproximateProperty],
      objectReps: Int,
      inputReps: Int,
      falsePositiveRate: Double
  ): Prop = {
    require(0 <= falsePositiveRate && falsePositiveRate <= 1)

    val list = a.flatMap { approximateProp =>
      successesAndProbabilities(approximateProp, objectReps, inputReps)
    }
    val monoid = implicitly[Monoid[(Int, Double, List[String])]]
    val (successes, sumOfProbabilities, _) = monoid.sum(list)
    val n = list.length

    (sumOfProbabilities - successes) > scala.math.sqrt(n * scala.math.log(falsePositiveRate) / -2)
  }
}

/**
 * All tests that use ApproximateProperty should extend from this class so that the scalacheck property is run
 * exactly once.
 */
abstract class ApproximateProperties(name: String) extends Properties(name) {
  override def overrideParameters(p: Test.Parameters): Test.Parameters =
    p.withMinSuccessfulTests(1)
}
