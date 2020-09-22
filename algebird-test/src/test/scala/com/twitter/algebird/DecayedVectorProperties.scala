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

import org.scalacheck.{Arbitrary, Gen}

class DecayedVectorProperties extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit val mpint: Arbitrary[DecayedVector[Map[Int, *]]] = Arbitrary {
    for {
      t <- Gen.choose(1e-4, 200.0) // Not too high so as to avoid numerical issues
      m <- Gen.mapOf(Gen.zip(Gen.choose(0, 100), Gen.choose(-1e5, 1e5)))
    } yield DecayedVector.forMap(m, t)
  }

  // TODO: we won't need this when we have an Equatable trait
  def decayedMapEqFn(
      a: DecayedVector[Map[Int, *]],
      b: DecayedVector[Map[Int, *]]
  ): Boolean = {

    def beCloseTo(a: Double, b: Double, eps: Double = 1e-5) =
      a == b ||
        ((2.0 * math.abs(a - b)) / (math.abs(a) + math.abs(b))) < eps ||
        (a.isInfinite && b.isInfinite) ||
        (a.isNaN && b.isNaN)

    val mapsAreClose = (a.vector.keySet ++ b.vector.keySet).forall { key =>
      (a.vector.get(key), b.vector.get(key)) match {
        case (Some(aVal), Some(bVal)) => beCloseTo(aVal, bVal)
        case (Some(aVal), None)       => beCloseTo(aVal, 0.0)
        case (None, Some(bVal))       => beCloseTo(bVal, 0.0)
        case _                        => true
      }
    }
    val timesAreClose = beCloseTo(a.scaledTime, b.scaledTime)
    mapsAreClose && timesAreClose
  }

  property("DecayedVector[Map[Int, *]] is a monoid") {
    implicit val equiv = Equiv.fromFunction(decayedMapEqFn)
    monoidLaws[DecayedVector[Map[Int, *]]]
  }
}
