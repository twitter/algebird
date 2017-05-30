package com.twitter.algebird

/**
 * Curve is a list of Confusion Matrices with different
 * thresholds
 *
 * @param matrices List of Matrices
 */
case class Curve(matrices: List[ConfusionMatrix])

/**
 * Given a List of (x,y) this functions computes the
 * Area Under the Curve
 */
object AreaUnderCurve {
  private def trapezoid(points: Seq[(Double, Double)]): Double = {
    require(points.length == 2)
    val x = points.head
    val y = points.last
    (y._1 - x._1) * (y._2 + x._2) / 2.0
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
case object CurveMonoid extends Monoid[Curve] {
  def zero = Curve(Nil)
  override def plus(left: Curve, right: Curve): Curve = {
    val sg = BinaryClassificationConfusionMatrixMonoid
    Curve(
      left.matrices.zip(right.matrices)
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
  extends Aggregator[BinaryPrediction, Curve, Double]
  with Serializable {

  private def linspace(a: Double, b: Double, length: Int = 100): Array[Double] = {
    val increment = (b - a) / (length - 1)
    Array.tabulate(length)(i => a + increment * i)
  }

  private lazy val thresholds = linspace(0.0, 1.0, samples)
  private lazy val aggregators = thresholds.map(BinaryClassificationConfusionMatrixAggregator(_)).toList

  def prepare(input: BinaryPrediction): Curve = Curve(aggregators.map(_.prepare(input)))

  def semigroup: Semigroup[Curve] = CurveMonoid

  def present(c: Curve): Double = {
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
