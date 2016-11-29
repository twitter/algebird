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

import org.scalacheck.{ Arbitrary, Prop }
import org.scalacheck.Prop.forAll

import scala.math.Equiv

/**
 * Properties useful for testing instances of Metric[T].
 */
trait MetricProperties {
  private def beCloseTo(a: Double, b: Double, eps: Double = 1e-10): Boolean =
    a == b || (math.abs(a - b) / math.abs(a)) < eps || (a.isInfinite && b.isInfinite)

  private def beGreaterThan(a: Double, b: Double, eps: Double = 1e-10): Boolean =
    a > b - eps || (a.isInfinite && b.isInfinite)

  def isNonNegative[T: Metric: Arbitrary]: Prop =
    forAll { (a: T, b: T) =>
      val m = Metric(a, b)
      beGreaterThan(m, 0.0) || beCloseTo(m, 0.0)
    }

  def isEqualIffZero[T: Metric: Arbitrary: Equiv]: Prop =
    forAll { (a: T, b: T) =>
      if (Equiv[T].equiv(a, b)) beCloseTo(Metric(a, b), 0.0)
      else !beCloseTo(Metric(a, b), 0.0)
    }

  def isSymmetric[T: Metric: Arbitrary]: Prop =
    forAll { (a: T, b: T) =>
      beCloseTo(Metric(a, b), Metric(b, a))
    }

  def satisfiesTriangleInequality[T: Metric: Arbitrary]: Prop =
    forAll { (a: T, b: T, c: T) =>
      val m1 = Metric(a, b) + Metric(b, c)
      val m2 = Metric(a, c)
      beGreaterThan(m1, m2) || beCloseTo(m1, m2)
    }

  def metricLaws[T: Metric: Arbitrary: Equiv]: Prop =
    isNonNegative[T] && isEqualIffZero[T] &&
      isSymmetric[T] && satisfiesTriangleInequality[T]
}
