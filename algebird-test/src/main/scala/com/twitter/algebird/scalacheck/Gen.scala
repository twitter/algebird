/*
Copyright 2016 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.twitter.algebird
package scalacheck

import org.scalacheck.Gen
import Gen._

/**
 * Gen instances for Algebird data structures.
 */
object gen extends ExpHistGen with IntervalGen {
  def firstGen[T](g: Gen[T]): Gen[First[T]] = g.map(First(_))

  def lastGen[T](g: Gen[T]): Gen[Last[T]] = g.map(Last(_))

  def genAdjoined[T](g: Gen[T]): Gen[AdjoinedUnit[T]] =
    g.map(AdjoinedUnit(_))

  def genDecayedValue: Gen[DecayedValue] =
    for {
      a <- choose(-1e100, 1e100) // Don't get too big and overflow
      b <- choose(-1e100, 1e100) // Don't get too big and overflow
    } yield DecayedValue(a, b)

  def genAveragedValue: Gen[AveragedValue] =
    for {
      cnt <- choose(Int.MinValue.toLong, Int.MaxValue.toLong)
      v <- choose(-1e100, 1e100) // Don't get too big and overflow
    } yield AveragedValue(cnt, v)

  def genMoments: Gen[Moments] =
    for {
      m0 <- choose(1L, Int.MaxValue.toLong)
      m1 <- choose(-1e50, 1e50)
      m2 <- choose(0, 1e50)
      m3 <- choose(-1e10, 1e50)
      m4 <- choose(0, 1e50)
    } yield new Moments(m0, m1, m2, m3, m4)

  def genStringSpaceSaver: Gen[SpaceSaver[String]] =
    Gen.frequency((1, genSSOneSpaceSaver), (10, genSSManySpaceSaver))

  def genSSOneSpaceSaver: Gen[SpaceSaver[String]] =
    for {
      capacity <- choose(2, 100)
      item <- Gen.alphaStr
    } yield SpaceSaver(capacity, item)

  def genFixedSSOneSpaceSaver: Gen[SpaceSaver[String]] =
    for {
      item <- Gen.alphaStr
    } yield SpaceSaver(10, item)

  def genSSManySpaceSaver: Gen[SpaceSaver[String]] =
    Gen.nonEmptyListOf(genFixedSSOneSpaceSaver).flatMap(l => l.reduce(_ ++ _))

  lazy val genCorrelation: Gen[Correlation] = {
    val recur = Gen.lzy(genCorrelation)

    // we can start with any pair of numbers:
    val genClose: Gen[Correlation] = for {
      x <- choose(-1000, 1000)
      delta <- choose(-100.0, 100.0)
    } yield Correlation((x, x + delta))

    val genUncorr: Gen[Correlation] = for {
      x <- choose(-1e10, 1e10)
      y <- choose(-1e10, 1e10)
    } yield Correlation((x, y))

    val genSum = Gen.zip(recur, recur).map { case (a, b) => CorrelationGroup.plus(a, b) }
    val genDiff = Gen.zip(recur, recur).map { case (a, b) => CorrelationGroup.minus(a, b) }
    val genNeg = recur.map(CorrelationGroup.negate)

    // now return with a low probability of choosing the branching cases:
    Gen
      .frequency((5, genClose), (5, genUncorr), (2, genNeg), (1, CorrelationGroup.zero), (1, genSum), (1, genDiff))
  }
}
