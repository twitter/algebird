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

import org.scalacheck._
import org.scalacheck.Prop._

object BaseScalingOperatorProperties extends Properties("ScalingOperator") with LinearScalingOperatorProperties {
  property("double scaling") = linearScalingLaws[Double](beCloseTo(_, _))
  property("float scaling") = linearScalingLaws[Float]((f1, f2) => beCloseTo(f1.toDouble, f2.toDouble))
}

trait LinearScalingOperatorProperties {
  def isEqualIfZero[T : ScalingOperator : Monoid : Arbitrary](eqfn: (T, T) => Boolean) = forAll { (a: T) =>
    eqfn(ScalingOperator.scale(a, 0.0), Monoid.zero[T])
  }

  def distributesWithPlus[T : ScalingOperator : Monoid : Arbitrary](eqfn: (T, T) => Boolean) = forAll { (a: T, b: T, c: Double) =>
    val v1 = ScalingOperator.scale(Monoid.plus(a, b), c)
    val v2 = Monoid.plus(ScalingOperator.scale(a, c), ScalingOperator.scale(b, c))
    eqfn(v1, v2)
  }

  def linearScalingLaws[T : ScalingOperator : Monoid : Arbitrary](eqfn: (T, T) => Boolean) =
    isEqualIfZero[T](eqfn) && distributesWithPlus[T](eqfn)

  def beCloseTo(a: Double, b: Double) =
    a == b || (math.abs(a - b) / math.abs(a)) < 1e-10 || (a.isInfinite && b.isInfinite) || a.isNaN || b.isNaN
}
