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
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

object DecayedVectorProperties extends Properties("DecayedVector") with BaseProperties {

  implicit val mpint: Arbitrary[DecayedVector[Double, ({type x[a]=Map[Int, a]})#x]] = Arbitrary { for {
      t <- choose(0.0, 1000.0)
      m <- arbitrary[Map[Int, Double]]
    } yield DecayedVector.forMap(m, t) }

  // TODO: we won't need this when we have an Equatable trait
  def decayedMapEqFn(a: DecayedVector[Double, ({type x[a]=Map[Int, a]})#x], b: DecayedVector[Double, ({type x[a]=Map[Int, a]})#x]) = {
    def beCloseTo(a: Double, b: Double) =
      a == b || (math.abs(a - b) / math.abs(a)) < 1e-10 || (a.isInfinite && b.isInfinite) || a.isNaN || b.isNaN
    val mapsAreClose = (a.vector.keySet ++ b.vector.keySet).forall { key =>
      (a.vector.get(key), b.vector.get(key)) match {
        case (Some(aVal), Some(bVal)) => beCloseTo(aVal, bVal)
        case (Some(aVal), None) => beCloseTo(aVal, 0.0)
        case (None, Some(bVal)) => beCloseTo(bVal, 0.0)
        case _ => true
      }
    }
    val timesAreClose = beCloseTo(a.expTime, b.expTime)
    mapsAreClose && timesAreClose
  }

  property("for Map[Int, Double] is a monoid") = monoidLawsEq[DecayedVector[Double, ({type x[a]=Map[Int, a]})#x]](decayedMapEqFn)
}
