package com.twitter.algebird.scalacheck

import org.apache.commons.statistics.inference.{ChiSquareTest, SignificanceResult}
import org.scalacheck.Prop.forAllNoShrink
import org.scalacheck.{Gen, Prop}

import scala.collection.mutable

/**
 * ScalaCheck properties for probabilistic testing.
 *
 * For randomized algorithms, we want to verify that the output follows the expected distribution. The
 * [[forAllSampled]] properties execute the test code a speecified number of times, collect results and
 * perform a chi-squared test.
 *
 * These properties do not shrink their input generators, as this is less useful for probabilistic tests.
 *
 * @param expectedFreq
 *   A map of outputs (results of the test code) to their expected frequencies. Frequencies do not have to add
 *   up to 1, only their relative size matters. They can all be equal to 1 if a uniform distribution is
 *   expected.
 *
 * @param alpha
 *   the significance level for the chi-squared test
 *
 * @tparam T
 *   the result type of the test code
 */
class Distribution[T](val expectedFreq: Map[T, Double], val alpha: Double) {
  private val samples: mutable.Map[T, Long] = mutable.Map().withDefaultValue(0)

  def collect(t: T): Unit = samples(t) += 1

  def isNotRejected: Boolean = !chiSquareTest.reject(alpha)

  private def chiSquareTest: SignificanceResult = {
    val (expected, observed) = expectedFreq.toSeq.map { case (k, v) => (v, samples(k)) }.toArray.unzip
    ChiSquareTest.withDefaults.test(expected, observed)
  }
}

object Distribution {
  private val defaultSigLevel = 0.001

  implicit def propFromDistribution[T](d: Distribution[T]): Prop =
    d.isNotRejected || Prop.falsified :| s"\u03a7\u00b2 test rejected the null hypothesis at \u03b1=${d.alpha} (p=${d.chiSquareTest.getPValue})"

  def uniform(n: Int): Map[Int, Double] = (0 until n).map(_ -> 1d).toMap

  /**
   * Runs the code block for the specified number of trials and verifies that the output follows the expected
   * distribution. The propoerty passes if a chi-squared test fails to reject the null hypothesis that the
   * distribution is the expected one at the given significance level.
   *
   * @param trials
   *   the number of iterations
   * @param alpha
   *   the significance level
   * @param expect
   *   A function computing the map of outputs (possible results) to their expected frequencies. For the
   *   overloaded versions of this method taking generator parameters, this function takes the generated
   *   values as input.
   * @param f
   *   the test code
   * @tparam T
   *   the result type
   * @return
   *   a [[Distribution]] object that can be used as a ScalaCheck property
   */
  def forAllSampled[T](trials: Int, alpha: Double = defaultSigLevel)(
      expect: Map[T, Double]
  )(f: => T): Prop = {
    val d = new Distribution(expect, alpha)
    (0 until trials).foreach { _ =>
      d.collect(f)
    }
    d
  }

  def forAllSampled[T1, T](trials: Int, g1: Gen[T1])(expect: T1 => Map[T, Double])(f: T1 => T): Prop =
    forAllNoShrink(g1)(t1 => forAllSampled(trials)(expect(t1))(f(t1)))

  def forAllSampled[T1, T2, T](trials: Int, g1: Gen[T1], g2: Gen[T2])(expect: (T1, T2) => Map[T, Double])(
      f: (T1, T2) => T
  ): Prop = forAllNoShrink(g1)(t1 => forAllSampled(trials, g2)(expect(t1, _: T2))(f(t1, _: T2)))

  def forAllSampled[T1, T2, T3, T](trials: Int, g1: Gen[T1], g2: Gen[T2], g3: Gen[T3])(
      expect: (T1, T2, T3) => Map[T, Double]
  )(f: (T1, T2, T3) => T): Prop =
    forAllNoShrink(g1, g2)((t1, t2) => forAllSampled(trials, g3)(expect(t1, t2, _: T3))(f(t1, t2, _: T3)))

  def forAllSampled[T1, T2, T3, T4, T](trials: Int, g1: Gen[T1], g2: Gen[T2], g3: Gen[T3], g4: Gen[T4])(
      expect: (T1, T2, T3, T4) => Map[T, Double]
  )(f: (T1, T2, T3, T4) => T): Prop = forAllNoShrink(g1, g2, g3)((t1, t2, t3) =>
    forAllSampled(trials, g4)(expect(t1, t2, t3, _: T4))(f(t1, t2, t3, _: T4))
  )

  def forAllSampled[T1, T2, T3, T4, T5, T](
      trials: Int,
      g1: Gen[T1],
      g2: Gen[T2],
      g3: Gen[T3],
      g4: Gen[T4],
      g5: Gen[T5]
  )(expect: (T1, T2, T3, T4, T5) => Map[T, Double])(f: (T1, T2, T3, T4, T5) => T): Prop =
    forAllNoShrink(g1, g2, g3, g4)((t1, t2, t3, t4) =>
      forAllSampled(trials, g5)(expect(t1, t2, t3, t4, _: T5))(f(t1, t2, t3, t4, _: T5))
    )

  def forAllSampled[T1, T2, T3, T4, T5, T6, T](
      trials: Int,
      g1: Gen[T1],
      g2: Gen[T2],
      g3: Gen[T3],
      g4: Gen[T4],
      g5: Gen[T5],
      g6: Gen[T6]
  )(expect: (T1, T2, T3, T4, T5, T6) => Map[T, Double])(f: (T1, T2, T3, T4, T5, T6) => T): Prop =
    forAllNoShrink(g1, g2, g3, g4, g5)((t1, t2, t3, t4, t5) =>
      forAllSampled(trials, g6)(expect(t1, t2, t3, t4, t5, _: T6))(f(t1, t2, t3, t4, t5, _: T6))
    )

  def forAllSampled[T1, T2, T3, T4, T5, T6, T7, T](
      trials: Int,
      g1: Gen[T1],
      g2: Gen[T2],
      g3: Gen[T3],
      g4: Gen[T4],
      g5: Gen[T5],
      g6: Gen[T6],
      g7: Gen[T7]
  )(expect: (T1, T2, T3, T4, T5, T6, T7) => Map[T, Double])(f: (T1, T2, T3, T4, T5, T6, T7) => T): Prop =
    forAllNoShrink(g1, g2, g3, g4, g5, g6)((t1, t2, t3, t4, t5, t6) =>
      forAllSampled(trials, g7)(expect(t1, t2, t3, t4, t5, t6, _: T7))(f(t1, t2, t3, t4, t5, t6, _: T7))
    )

  def forAllSampled[T1, T2, T3, T4, T5, T6, T7, T8, T](
      trials: Int,
      g1: Gen[T1],
      g2: Gen[T2],
      g3: Gen[T3],
      g4: Gen[T4],
      g5: Gen[T5],
      g6: Gen[T6],
      g7: Gen[T7],
      g8: Gen[T8]
  )(expect: (T1, T2, T3, T4, T5, T6, T7, T8) => Map[T, Double])(
      f: (T1, T2, T3, T4, T5, T6, T7, T8) => T
  ): Prop = forAllNoShrink(g1, g2, g3, g4, g5, g6, g7)((t1, t2, t3, t4, t5, t6, t7) =>
    forAllSampled(trials, g8)(expect(t1, t2, t3, t4, t5, t6, t7, _: T8))(
      f(t1, t2, t3, t4, t5, t6, t7, _: T8)
    )
  )
}
