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

/**
 * A BinaryPrediction is a label with a score
 *
 * @param score Score of the classifier
 * @param label Is this in the positive or negative class.
 */
case class BinaryPrediction(score: Double, label: Boolean) extends Serializable {
  override def toString: String = s"$label,$score"
}

/**
 * Confusion Matrix itself with the statistics to be aggregated
 */
case class ConfusionMatrix(
  truePositive: Int = 0,
  falsePositive: Int = 0,
  falseNegative: Int = 0,
  trueNegative: Int = 0)
  extends Serializable

/**
 * After the aggregation this generates some common statistics
 *
 * @param fscore F Score based on the alpha given to the Aggregator
 * @param precision Precision Score
 * @param recall Recall Score
 * @param falsePositiveRate False Positive Rate
 * @param matrix Confusion Matrix
 */
case class Scores(
  fscore: Double,
  precision: Double,
  recall: Double,
  falsePositiveRate: Double,
  matrix: ConfusionMatrix)
  extends Serializable

case object BinaryClassificationConfusionMatrixMonoid extends Monoid[ConfusionMatrix] {
  def zero: ConfusionMatrix = ConfusionMatrix()
  override def plus(left: ConfusionMatrix, right: ConfusionMatrix): ConfusionMatrix = {
    val tp = left.truePositive + right.truePositive
    val fp = left.falsePositive + right.falsePositive
    val fn = left.falseNegative + right.falseNegative
    val tn = left.trueNegative + right.trueNegative

    ConfusionMatrix(tp, fp, fn, tn)
  }
}

/**
 * A Confusion Matrix Aggregator creates a Confusion Matrix and
 * relevant scores for a given threshold given predictions from
 * a binary classifier.
 *
 * @param threshold Threshold to use for the predictions
 * @param beta Beta used in the FScore Calculation.
 */
case class BinaryClassificationConfusionMatrixAggregator(threshold: Double = 0.5, beta: Double = 1.0)
  extends Aggregator[BinaryPrediction, ConfusionMatrix, Scores]
  with Serializable {

  def prepare(input: BinaryPrediction): ConfusionMatrix =
    (input.label, input.score) match {
      case (true, score) if score > threshold =>
        ConfusionMatrix(truePositive = 1)
      case (true, score) if score < threshold =>
        ConfusionMatrix(falseNegative = 1)
      case (false, score) if score < threshold =>
        ConfusionMatrix(trueNegative = 1)
      case (false, score) if score > threshold =>
        ConfusionMatrix(falsePositive = 1)
    }

  def semigroup: Semigroup[ConfusionMatrix] =
    BinaryClassificationConfusionMatrixMonoid

  def present(m: ConfusionMatrix): Scores = {
    val precDenom = m.truePositive.toDouble + m.falsePositive.toDouble
    val precision = if (precDenom > 0.0) m.truePositive.toDouble / precDenom else 1.0

    val recallDenom = m.truePositive.toDouble + m.falseNegative.toDouble
    val recall = if (recallDenom > 0.0) m.truePositive.toDouble / recallDenom else 1.0

    val fpDenom = m.falsePositive.toDouble + m.trueNegative.toDouble
    val fpr = if (fpDenom > 0.0) m.falsePositive.toDouble / fpDenom else 0.0

    val betaSqr = Math.pow(beta, 2.0)

    val fScoreDenom = (betaSqr * precision) + recall

    val fscore = if (fScoreDenom > 0.0) {
      (1 + betaSqr) * ((precision * recall) / fScoreDenom)
    } else { 1.0 }

    Scores(fscore, precision, recall, fpr, m)
  }
}
