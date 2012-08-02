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

// Represents a decayed value that is decayed of the form:
// \sum_i e^{-(t_i - t)} v_i
// 2^{-(t/th)} = exp(ln(2)(-t/th)) = exp(-t * (ln(2)/th))
// So time is measured in units of (half-life/ln(2)), so.
// t in seconds, 1 day half life means: t => t * ln(2)/(86400.0)

object DecayedValue extends java.io.Serializable {
  def build[V <% Double](value : V, time : Double, halfLife : Double) = {
    DecayedValue(value, time * scala.math.log(2.0)/halfLife)
  }
  val zero = DecayedValue(0.0, Double.NegativeInfinity)
  def scale(newv : DecayedValue, oldv : DecayedValue, eps : Double) = {
    val newValue = newv.value +
        scala.math.exp(oldv.scaledTime - newv.scaledTime) * oldv.value
    if( newValue > eps ) {
      DecayedValue(newValue, newv.scaledTime)
    }
    else {
      zero
    }
  }

  def monoidWithEpsilon(eps : Double) = new Monoid[DecayedValue] {
    override val zero = DecayedValue(0.0, Double.NegativeInfinity)
    override def plus(left : DecayedValue, right : DecayedValue) = {
      if (left < right) {
        //left is older:
        scale(right, left, eps)
      }
      else {
        // right is older
        scale(left, right, eps)
      }
    }
  }
}

case class DecayedValue(value : Double, scaledTime : Double) extends Ordered[DecayedValue] {
  def compare(that : DecayedValue) : Int = {
    scaledTime.compareTo(that.scaledTime)
  }
}

object AveragedValue {
  def apply[V <% Double](v : V) = new AveragedValue(1L, v)
  def apply[V <% Double](c : Long, v : V) = new AveragedValue(c, v)
}


case class AveragedValue(count : Long, value : Double)

object AveragedMonoid extends Monoid[AveragedValue] {
  // When combining averages, if the counts sizes are too close we should use a different
  // algorithm.  This constant defines how close the ratio of the smaller to the total count
  // can be:
  private val STABILITY_CONSTANT = 0.1
  /**
   * uses a more stable online algorithm which should
   * be suitable for large numbers of records
   * similar to:
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   */
  val zero = AveragedValue(0L, 0.0)
  def plus(cntAve1 : AveragedValue, cntAve2 : AveragedValue) : AveragedValue = {
    val (big, small) = if (cntAve1.count >= cntAve2.count)
        (cntAve1, cntAve2)
      else
        (cntAve2, cntAve1)
    val n = big.count
    val k = small.count
    if (k == 0L) {
      // Handle zero without allocation
      big
    }
    else {
      val an = big.value
      val ak = small.value
      val newCnt = n+k
      val scaling = k.toDouble/newCnt
      // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
      val newAve = if (scaling < STABILITY_CONSTANT) (an + (ak - an)*scaling) else (n*an + k*ak)/newCnt
      new AveragedValue(newCnt, newAve)
    }
  }
}

/**
 * This is an associative, but not commutative monoid
 * Also, you must start on the right, with a value, and all subsequent RightFolded must
 * be RightFoldedToFold objects or zero
 *
 * If you add to Folded values together, you always get the one on the left,
 * so this forms a kind of reset of the fold.
 */
object RightFolded {
  def monoid[In,Out](foldfn : (In,Out) => Out) =
    new Monoid[RightFolded[In,Out]] {

    lazy val zero = RightFoldedZero[In,Out]()

    def plus(left : RightFolded[In,Out], right : RightFolded[In,Out]) = {
      right match {
        case RightFoldedZero() => left
        case RightFoldedValue(vr) => {
          left match {
            case RightFoldedZero() => right
            case RightFoldedToFold(l) => RightFoldedValue(l.foldRight(vr)(foldfn))
            case RightFoldedValue(_) => left
          }
        }
        case RightFoldedToFold(rightList) => {
          left match {
            case RightFoldedZero() => right
            case RightFoldedToFold(lList) => RightFoldedToFold(lList ++ rightList)
            case RightFoldedValue(_) => left
          }
        }
      }
    }
  }
}

sealed abstract class RightFolded[In,Out]
case class RightFoldedZero[In,Out]() extends RightFolded[In,Out]
case class RightFoldedValue[In,Out](v : Out) extends RightFolded[In,Out]
case class RightFoldedToFold[In,Out](in : List[In]) extends RightFolded[In,Out]
