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

import org.scalacheck.{ Gen, Arbitrary }

class VectorSpaceProperties extends CheckProperties {
  import com.twitter.algebird.BaseVectorSpaceProperties._

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

  implicit val genDouble = Arbitrary{ Gen.choose(-1.0E50, 1.0E50) }

  property("map int double scaling") {
    vectorSpaceLaws[Double, ({ type x[a] = Map[Int, a] })#x](mapEqFn(_, _))
  }
}
