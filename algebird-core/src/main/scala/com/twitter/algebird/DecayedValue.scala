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
  def build[V <% Double](value: V, time: Double, halfLife: Double) = {
    DecayedValue(value, time * scala.math.log(2.0) / halfLife)
  }
  val zero = DecayedValue(0.0, Double.NegativeInfinity)

  def scale(newv: DecayedValue, oldv: DecayedValue, eps: Double) = {
    val newValue = newv.value +
      scala.math.exp(oldv.scaledTime - newv.scaledTime) * oldv.value
    if (scala.math.abs(newValue) > eps) {
      DecayedValue(newValue, newv.scaledTime)
    } else {
      zero
    }
  }

  def monoidWithEpsilon(eps: Double): Monoid[DecayedValue] = new DecayedValueMonoid(eps)
}

case class DecayedValueMonoid(eps: Double) extends Monoid[DecayedValue] {
  override val zero = DecayedValue(0.0, Double.NegativeInfinity)
  override def plus(left: DecayedValue, right: DecayedValue) =
    if (left < right) {
      // left is older:
      DecayedValue.scale(right, left, eps)
    } else {
      // right is older
      DecayedValue.scale(left, right, eps)
    }

  // Returns value if timestamp is less than value's timestamp
  def valueAsOf(value: DecayedValue, halfLife: Double, timestamp: Double): Double = {
    plus(DecayedValue.build(0, timestamp, halfLife), value).value
  }
}

case class DecayedValue(value: Double, scaledTime: Double) extends Ordered[DecayedValue] {
  def compare(that: DecayedValue): Int = {
    scaledTime.compareTo(that.scaledTime)
  }

  // A DecayedValue can be translated to a moving average with the window size of its half-life.
  //   It is EXACTLY a sample of the Laplace transform of the signal of values.
  //   Therefore, we can get the moving average by normalizing a decayed value with halflife/ln(2),
  //   which is the integral of exp(-t(ln(2))/halflife) from 0 to infinity.
  //
  //   See: https://github.com/twitter/algebird/wiki/Using-DecayedValue-as-moving-average
  def average(halfLife: Double) = {
    val normalization = halfLife / math.log(2)
    value / normalization
  }

  /*
  * Moving average assuming the signal started at zero a fixed point in the past.
  * This normalizes by the integral of exp(-t(ln(2))/halflife) from 0 to (endTime - startTime).
  */
  def averageFrom(halfLife: Double, startTime: Double, endTime: Double) = {
    if(endTime > startTime) {
      val asOfEndTime = DecayedValue.scale(DecayedValue.build(0, endTime, halfLife), this, 0.0)
      val timeDelta = startTime - endTime
      val normalization = halfLife * (1 - math.pow(2, timeDelta / halfLife)) / math.log(2)
      asOfEndTime.value / normalization
    } else {
      0.0
    }
  }
}
