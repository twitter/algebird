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
 * Curve is a list of Confusion Matrices with different
 * thresholds
 *
 * @param matrices List of Matrices
 */
case class ConfusionCurve(matrices: List[ConfusionMatrix])

/**
 * Given a List of (x,y) this functions computes the
 * Area Under the Curve
 */
object AreaUnderCurve {
  private def trapezoid(points: Seq[(Double, Double)]): Double =
    points match {
      case Seq((x1, x2), (y1, y2)) => (y1 - x1) * (y2 + x2) / 2.0
      case _ => sys.error("Points must be of length 2.")
    }

  def of(curve: List[(Double, Double)]): Double = {
    curve.toIterator.sliding(2).withPartial(false).aggregate(0.0)(
      seqop = (auc: Double, points: Seq[(Double, Double)]) => auc + trapezoid(points),
      combop = _ + _)
  }
}

sealed trait AUCMetric
case object ROC extends AUCMetric
case object PR extends AUCMetric

/**
 * Sums Curves which are a series of Confusion Matrices
 * with different thresholds
 */
case object ConfusionCurveMonoid extends Monoid[ConfusionCurve] {
  def zero = ConfusionCurve(Nil)
  override def plus(left: ConfusionCurve, right: ConfusionCurve): ConfusionCurve = {
    val sg = BinaryClassificationConfusionMatrixMonoid

    ConfusionCurve(
      left.matrices.zipAll(right.matrices, sg.zero, sg.zero)
        .map{ case (cl, cr) => sg.plus(cl, cr) })
  }
}

/**
 * AUCAggregator computes the Area Under the Curve
 * for a given metric by sampling along that curve.
 *
 * The number of samples is taken and is used to compute
 * the thresholds to use. A confusion matrix is then computed
 * for each threshold and finally that is used to compute the
 * Area Under the Curve.
 *
 * Note this is for Binary Classifications Tasks
 *
 * @param metric Which Metric to compute
 * @param samples Number of samples, defaults to 100
 */
case class BinaryClassificationAUCAggregator(metric: AUCMetric, samples: Int = 100)
  extends Aggregator[BinaryPrediction, ConfusionCurve, Double]
  with Serializable {

  private def linspace(a: Double, b: Double, length: Int): Array[Double] = {
    val increment = (b - a) / (length - 1)
    Array.tabulate(length)(i => a + increment * i)
  }

  private lazy val thresholds = linspace(0.0, 1.0, samples)
  private lazy val aggregators = thresholds.map(BinaryClassificationConfusionMatrixAggregator(_)).toList

  def prepare(input: BinaryPrediction): ConfusionCurve = ConfusionCurve(aggregators.map(_.prepare(input)))

  def semigroup: Semigroup[ConfusionCurve] = ConfusionCurveMonoid

  def present(c: ConfusionCurve): Double = {
    val total = c.matrices.map { matrix =>
      val scores = BinaryClassificationConfusionMatrixAggregator().present(matrix)
      metric match {
        case ROC => (scores.falsePositiveRate, scores.recall)
        case PR => (scores.recall, scores.precision)
      }
    }.reverse

    val combined = metric match {
      case ROC => total ++ List((1.0, 1.0))
      case PR => List((0.0, 1.0)) ++ total
    }

    AreaUnderCurve.of(combined)
  }
}

object BinaryClassificationAUC {
  implicit def monoid: Monoid[ConfusionCurve] = ConfusionCurveMonoid
}
