/*
Copyright 2012 Twitter, Inc.

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

import org.scalacheck.Arbitrary
import org.scalacheck.Gen.choose
import org.scalactic.TolerantNumerics
import org.scalatest.{ Matchers, WordSpec }

class BinaryClassificationConfusionMatrixMonoidLaws extends CheckProperties {
  import BaseProperties._

  implicit val semigroup = BinaryClassificationConfusionMatrixMonoid
  implicit val gen = Arbitrary {
    for (
      v <- choose(0, 10000)
    ) yield ConfusionMatrix(truePositive = v)
  }

  property("ConfusionMatrix is associative") {
    isAssociative[ConfusionMatrix]
  }

  property("ConfusionMatrix is a monoid") {
    monoidLaws[ConfusionMatrix]
  }
}

class BinaryClassificationConfusionMatrixTest extends WordSpec with Matchers {
  lazy val data =
    List(
      BinaryPrediction(0.1, false),
      BinaryPrediction(0.1, true),
      BinaryPrediction(0.4, false),
      BinaryPrediction(0.6, false),
      BinaryPrediction(0.6, true),
      BinaryPrediction(0.6, true),
      BinaryPrediction(0.8, true))

  "BinaryClassificationConfusionMatrix" should {
    implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(0.1)

    "return a correct confusion matrix" in {
      val aggregator = BinaryClassificationConfusionMatrixAggregator()
      val scored = aggregator(data)

      assert(scored.recall === 0.75)
      assert(scored.precision === 0.75)
      assert(scored.fscore === 0.75)
      assert(scored.falsePositiveRate === 0.333)
    }
  }
}
