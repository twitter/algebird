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

import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll

/**
 * Base properties for VectorSpace tests.
 */

object BaseVectorSpaceProperties {
  def isEqualIfZero[F, C[_]](eqfn: (C[F], C[F]) => Boolean)(implicit vs: VectorSpace[F, C], arb: Arbitrary[C[F]]) =
    forAll { (a: C[F]) =>
      eqfn(VectorSpace.scale(vs.field.zero, a), vs.group.zero)
    }

  def distributesWithPlus[F, C[_]](eqfn: (C[F], C[F]) => Boolean)(implicit vs: VectorSpace[F, C], arbC: Arbitrary[C[F]], arbF: Arbitrary[F]) =
    forAll { (a: C[F], b: C[F], c: F) =>
      val v1 = VectorSpace.scale(c, vs.group.plus(a, b))
      val v2 = vs.group.plus(VectorSpace.scale(c, a), VectorSpace.scale(c, b))
      eqfn(v1, v2)
    }

  def isAssociative[F, C[_]](eqfn: (C[F], C[F]) => Boolean)(implicit vs: VectorSpace[F, C], arbC: Arbitrary[C[F]], arbF: Arbitrary[F]) =
    forAll { (a: C[F], b: F, c: F) =>
      val v1 = VectorSpace.scale(c, VectorSpace.scale(b, a))
      val v2 = VectorSpace.scale(vs.field.times(c, b), a)
      eqfn(v1, v2)
    }

  def identityOne[F, C[_]](eqfn: (C[F], C[F]) => Boolean)(implicit vs: VectorSpace[F, C], arb: Arbitrary[C[F]]) =
    forAll { (a: C[F]) =>
      eqfn(VectorSpace.scale(vs.field.one, a), a)
    }

  def distributesOverScalarPlus[F, C[_]](eqfn: (C[F], C[F]) => Boolean)(implicit vs: VectorSpace[F, C], arbC: Arbitrary[C[F]], arbF: Arbitrary[F]) =
    forAll { (a: C[F], b: F, c: F) =>
      val v1 = VectorSpace.scale(vs.field.plus(b, c), a)
      val v2 = vs.group.plus(VectorSpace.scale(b, a), VectorSpace.scale(c, a))
      eqfn(v1, v2)
    }

  def vectorSpaceLaws[F, C[_]](eqfn: (C[F], C[F]) => Boolean)(implicit vs: VectorSpace[F, C], arbC: Arbitrary[C[F]], arbF: Arbitrary[F]) =
    isEqualIfZero(eqfn) && distributesWithPlus(eqfn) && isAssociative(eqfn) && identityOne(eqfn) && distributesOverScalarPlus(eqfn)

  def beCloseTo(a: Double, b: Double) =
    a == b || (math.abs(a - b) / math.abs(a)) < 1e-10 || (a.isInfinite && b.isInfinite) || a.isNaN || b.isNaN
}
