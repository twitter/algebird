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

  // TODO: we won't need this when we have an Equatable trait
  def mapEqFn(a: Map[Int, Double], b: Map[Int, Double]) = {
    (a.keySet ++ b.keySet).forall { key =>
      (a.get(key), b.get(key)) match {
        case (Some(aVal), Some(bVal)) => beCloseTo(aVal, bVal)
        case (Some(aVal), None) => beCloseTo(aVal, 0.0)
        case (None, Some(bVal)) => beCloseTo(bVal, 0.0)
        case _ => true
      }
    }
  }

  property("map int double scaling") = linearScalingLaws[Map[Int, Double]](mapEqFn(_, _))
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

  def isAssociative[T : ScalingOperator : Arbitrary](eqfn: (T, T) => Boolean) = forAll { (a: T, b: Double, c: Double) =>
    val v1 = ScalingOperator.scale(ScalingOperator.scale(a, b), c)
    val v2 = ScalingOperator.scale(a, b * c)
    eqfn(v1, v2)
  }

  def identityOne[T : ScalingOperator : Monoid : Arbitrary](eqfn: (T, T) => Boolean) = forAll { (a: T) =>
    eqfn(ScalingOperator.scale(a, 1.0), a)
  }

  def distributesOverScalarPlus[T : ScalingOperator : Monoid : Arbitrary](eqfn: (T, T) => Boolean) = forAll { (a: T, b: Double, c: Double) =>
    val v1 = ScalingOperator.scale(a, b + c)
    val v2 = Monoid.plus(ScalingOperator.scale(a, b), ScalingOperator.scale(a, c))
    eqfn(v1, v2)
  }

  def linearScalingLaws[T : ScalingOperator : Monoid : Arbitrary](eqfn: (T, T) => Boolean) =
    isEqualIfZero(eqfn) && distributesWithPlus(eqfn) && isAssociative(eqfn) && identityOne(eqfn) && distributesOverScalarPlus(eqfn)

  def beCloseTo(a: Double, b: Double) =
    a == b || (math.abs(a - b) / math.abs(a)) < 1e-10 || (a.isInfinite && b.isInfinite) || a.isNaN || b.isNaN
}
