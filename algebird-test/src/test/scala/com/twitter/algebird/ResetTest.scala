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
import org.scalacheck.Prop._

class ResetTest extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit def resetArb[T: Arbitrary]: Arbitrary[ResetState[T]] = Arbitrary {
    Arbitrary.arbitrary[T].map { t =>
      if (scala.math.random < 0.1) {
        ResetValue(t)
      } else {
        SetValue(t)
      }
    }
  }

  property("ResetState[Int] forms a Monoid") {
    monoidLaws[ResetState[Int]]
  }

  property("ResetState[String] forms a Monoid") {
    monoidLaws[ResetState[String]]
  }

  property("ResetState[Int] works as expected") {
    forAll { (a: ResetState[Int], b: ResetState[Int], c: ResetState[Int]) =>
      val result = Monoid.plus(Monoid.plus(a, b), c)
      ((a, b, c) match {
        case (SetValue(x), SetValue(y), SetValue(z))   => SetValue(x + y + z)
        case (ResetValue(x), SetValue(y), SetValue(z)) => ResetValue(x + y + z)
        case (_, ResetValue(y), SetValue(z))           => ResetValue(y + z)
        case (_, _, ResetValue(z))                     => ResetValue(z)
      }) == result
    }
  }
}
