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

import algebra.{CommutativeGroup, CommutativeMonoid}

/**
 * A class to calculate the first five central moments over a sequence of Doubles. Given the first five
 * central moments, we can then calculate metrics like skewness and kurtosis.
 *
 * m{i} denotes the ith central moment.
 *
 * This code manually inlines code to make it look like a case class. This is done because we changed the
 * count from a Long to a Double to enable the scale method, which allows exponential decays of moments, but
 * we didn't want to break backwards binary compatibility.
 */
sealed class Moments(val m0D: Double, val m1: Double, val m2: Double, val m3: Double, val m4: Double)
    extends Product
    with Serializable {
  def this(m0: Long, m1: Double, m2: Double, m3: Double, m4: Double) =
    this(m0.toDouble, m1, m2, m3, m4)

  def m0: Long = m0D.toLong

  def count: Long = m0

  def totalWeight: Double = m0D

  def mean: Double = m1

  // Population variance, not sample variance.
  def variance: Double =
    m2 / m0D

  // Population standard deviation, not sample standard deviation.
  def stddev: Double = math.sqrt(variance)

  def skewness: Double =
    math.sqrt(m0D) * m3 / math.pow(m2, 1.5)

  def kurtosis: Double =
    m0D * m4 / math.pow(m2, 2) - 3

  override def productArity: Int = 5
  override def productElement(idx: Int): Any =
    idx match {
      case 0 => count
      case 1 => m1
      case 2 => m2
      case 3 => m3
      case 4 => m4
    }

  override def canEqual(that: Any): Boolean =
    that.isInstanceOf[Moments]

  def copy(c0: Long = count, v1: Double = m1, v2: Double = m2, v3: Double = m3, v4: Double = m4): Moments = {
    val v0 = if (c0 == count) m0D else c0.toDouble
    new Moments(m0D = v0, m1 = v1, m2 = v2, m3 = v3, m4 = v4)
  }

  override def toString: String =
    s"Moments($m0D, $m1, $m2, $m3, $m4)"

  override def hashCode: Int = scala.util.hashing.MurmurHash3.productHash(this)

  override def equals(that: Any): Boolean =
    that match {
      case thatM: Moments =>
        (m0D == thatM.m0D) &&
          (m1 == thatM.m1) &&
          (m2 == thatM.m2) &&
          (m3 == thatM.m3) &&
          (m4 == thatM.m4)
      case _ => false
    }

  /**
   * Scale all the moments by a constant. This allows you to use Moments with exponential decay
   */
  def scale(z: Double): Moments =
    if (z < 0.0) // the "extraneous" if here is to avoid allocating the error message unless necessary
      throw new IllegalArgumentException(s"cannot scale by negative value: $z")
    else if (z == 0)
      Moments.momentsMonoid.zero
    else
      new Moments(m0D = z * m0D, m1 = m1, m2 = z * m2, m3 = z * m3, m4 = z * m4)
}

object Moments {
  @deprecated("use monoid[Moments], this isn't lawful for negate", "0.13.8")
  def group: Group[Moments] with CommutativeGroup[Moments] =
    MomentsGroup

  implicit val momentsMonoid: Monoid[Moments] with CommutativeMonoid[Moments] =
    new MomentsMonoid

  val aggregator: MomentsAggregator.type = MomentsAggregator

  def numericAggregator[N](implicit num: Numeric[N]): MonoidAggregator[N, Moments, Moments] =
    Aggregator.prepareMonoid { n: N => Moments(num.toDouble(n)) }

  /**
   * Create a Moments object given a single value. This is useful for initializing moment calculations at the
   * start of a stream.
   */
  def apply[V: Numeric](value: V)(implicit num: Numeric[V]): Moments =
    new Moments(1.0, num.toDouble(value), 0, 0, 0)

  def apply[V](m0: Long, m1: V, m2: V, m3: V, m4: V)(implicit num: Numeric[V]): Moments =
    new Moments(m0, num.toDouble(m1), num.toDouble(m2), num.toDouble(m3), num.toDouble(m4))

  /**
   * This it the legacy apply when count was a Long
   */
  def apply(m0: Long, m1: Double, m2: Double, m3: Double, m4: Double): Moments =
    new Moments(m0, m1, m2, m3, m4)

  /**
   * This it the legacy unapply when count was a Long
   */
  def unapply(m: Moments): Option[(Long, Double, Double, Double, Double)] =
    Some((m.m0, m.m1, m.m2, m.m3, m.m4))

  /**
   * When combining averages, if the counts sizes are too close we should use a different algorithm. This
   * constant defines how close the ratio of the smaller to the total count can be:
   */
  private[this] val STABILITY_CONSTANT = 0.1

  /**
   * Given two streams of doubles (weightN, an) and (weightK, ak) of form (weighted count, mean), calculates
   * the mean of the combined stream.
   *
   * Uses a more stable online algorithm which should be suitable for large numbers of records similar to:
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   *
   * This differs from the implementation in MomentsGroup.scala only in that here, the counts are weighted,
   * and are thus doubles instead of longs
   */
  def getCombinedMeanDouble(weightN: Double, an: Double, weightK: Double, ak: Double): Double =
    if (weightN < weightK) getCombinedMeanDouble(weightK, ak, weightN, an)
    else
      (weightN + weightK) match {
        case 0.0                             => 0.0
        case newCount if newCount == weightN => an
        case newCount =>
          val scaling = weightK / newCount
          // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
          if (scaling < STABILITY_CONSTANT) (an + (ak - an) * scaling)
          else (weightN * an + weightK * ak) / newCount
      }

}

class MomentsMonoid extends Monoid[Moments] with CommutativeMonoid[Moments] {

  /**
   * When combining averages, if the counts sizes are too close we should use a different algorithm. This
   * constant defines how close the ratio of the smaller to the total count can be:
   */
  private val STABILITY_CONSTANT = 0.1

  /**
   * Given two streams of doubles (n, an) and (k, ak) of form (count, mean), calculates the mean of the
   * combined stream.
   *
   * Uses a more stable online algorithm which should be suitable for large numbers of records similar to:
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   *
   * we no longer use this, but we can't remove it due to binary compatibility
   */
  @deprecated("Use Moments.getCombinedMeanDouble instead", since = "0.13.8")
  def getCombinedMean(n: Long, an: Double, k: Long, ak: Double): Double =
    if (n < k) getCombinedMean(k, ak, n, an)
    else
      (n + k) match {
        case 0L                        => 0.0
        case newCount if newCount == n => an
        case newCount =>
          val scaling = k.toDouble / newCount
          // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
          if (scaling < STABILITY_CONSTANT) (an + (ak - an) * scaling)
          else (n * an + k * ak) / newCount
      }

  override val zero: Moments = new Moments(0.0, 0.0, 0.0, 0.0, 0.0)

  // Combines the moment calculations from two streams.
  // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
  // for more information on the formulas used to update the moments.
  override def plus(a: Moments, b: Moments): Moments = {
    val countCombined = a.m0D + b.m0D
    if (countCombined == 0.0) zero
    else {
      val delta = b.mean - a.mean
      val meanCombined = Moments.getCombinedMeanDouble(a.m0D, a.mean, b.m0D, b.mean)

      val m2 = a.m2 + b.m2 +
        math.pow(delta, 2) * a.m0D * b.m0D / countCombined

      val m3 = a.m3 + b.m3 +
        math.pow(delta, 3) * a.m0D * b.m0D * (a.m0D - b.m0D) / math.pow(countCombined, 2) +
        3 * delta * (a.m0D * b.m2 - b.m0D * a.m2) / countCombined

      val m4 = a.m4 + b.m4 +
        math.pow(delta, 4) * a.m0D * b.m0D * (math.pow(a.m0D, 2) -
          a.m0D * b.m0D + math.pow(b.m0D, 2)) / math.pow(countCombined, 3) +
        6 * math.pow(delta, 2) * (math.pow(a.m0D, 2) * b.m2 +
          math.pow(b.m0D, 2) * a.m2) / math.pow(countCombined, 2) +
        4 * delta * (a.m0D * b.m3 - b.m0D * a.m3) / countCombined

      new Moments(countCombined, meanCombined, m2, m3, m4)
    }
  }

  override def sumOption(items: TraversableOnce[Moments]): Option[Moments] =
    if (items.isEmpty) None
    else {
      val iter = items.toIterator

      val init = iter.next()

      var count: Double = init.m0D
      var mean: Double = init.mean
      var m2: Double = init.m2
      var m3: Double = init.m3
      var m4: Double = init.m4

      while (iter.hasNext) {

        /*
         * Unfortunately we copy the code in plus, but we do
         * it to avoid allocating a new Moments on every item
         * in the loop. the Monoid laws test that sum
         * matches looping on plus
         */
        val b = iter.next()

        val countCombined = count + b.m0D

        if (countCombined == 0.0) {
          mean = 0.0
          m2 = 0.0
          m3 = 0.0
          m4 = 0.0
        } else {
          val delta = b.mean - mean
          val meanCombined = Moments.getCombinedMeanDouble(count, mean, b.m0D, b.mean)

          val m2Combined = m2 + b.m2 +
            math.pow(delta, 2) * count * b.m0D / countCombined

          val m3Combined = m3 + b.m3 +
            math.pow(delta, 3) * count * b.m0D * (count - b.m0D) / math.pow(countCombined, 2) +
            3 * delta * (count * b.m2 - b.m0D * m2) / countCombined

          val m4Combined = m4 + b.m4 +
            math.pow(delta, 4) * count * b.m0D * (math.pow(count, 2) -
              count * b.m0D + math.pow(b.m0D, 2)) / math.pow(countCombined, 3) +
            6 * math.pow(delta, 2) * (math.pow(count, 2) * b.m2 +
              math.pow(b.m0D, 2) * m2) / math.pow(countCombined, 2) +
            4 * delta * (count * b.m3 - b.m0D * m3) / countCombined

          mean = meanCombined
          m2 = m2Combined
          m3 = m3Combined
          m4 = m4Combined
        }

        count = countCombined
      }

      Some(new Moments(count, mean, m2, m3, m4))
    }
}

/**
 * This should not be used as a group (avoid negate and minus). It was wrongly believed that this was a group
 * for several years in this code, however it was only being tested with positive counts (which is to say the
 * generators were too weak). It isn't the case that minus and negate are totally wrong but (a - a) + b in
 * general isn't associative: it won't equal a - (a - b) which it should.
 */
@deprecated("use Moments.momentsMonoid, this isn't lawful for negative counts", "0.13.8")
object MomentsGroup extends MomentsMonoid with Group[Moments] with CommutativeGroup[Moments] {

  override def negate(a: Moments): Moments =
    new Moments(-a.m0D, a.m1, -a.m2, -a.m3, -a.m4)
}

object MomentsAggregator extends MonoidAggregator[Double, Moments, Moments] {
  override val monoid: MomentsGroup.type = MomentsGroup

  override def prepare(input: Double): Moments = Moments(input)
  override def present(m: Moments): Moments = m
}
