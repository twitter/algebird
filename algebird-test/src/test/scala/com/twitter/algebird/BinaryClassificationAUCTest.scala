package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Gen.choose
import org.scalactic.TolerantNumerics
import org.scalatest.{ Matchers, _ }

class CurveMonoidLaws extends CheckProperties {
  import BaseProperties._

  implicit val semigroup = CurveMonoid
  implicit val gen = Arbitrary {
    for (
      v <- choose(0, 10000)
    ) yield Curve(List(ConfusionMatrix(truePositive = v)))
  }

  property("Curve is associative") {
    isAssociative[Curve]
  }
}

class BinaryClassificationAUCTest extends WordSpec with Matchers {
  lazy val data =
    List(
      BinaryPrediction(0.1, false),
      BinaryPrediction(0.1, true),
      BinaryPrediction(0.4, false),
      BinaryPrediction(0.6, false),
      BinaryPrediction(0.6, true),
      BinaryPrediction(0.6, true),
      BinaryPrediction(0.8, true))

  "BinaryClassificationAUC" should {
    implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(0.1)

    "return roc auc" in {
      val aggregator = BinaryClassificationAUCAggregator(ROC, samples = 50)
      assert(aggregator(data) === 0.708)
    }

    "return pr auc" in {
      val aggregator = BinaryClassificationAUCAggregator(PR, samples = 50)
      assert(aggregator(data) === 0.833)
    }
  }
}
