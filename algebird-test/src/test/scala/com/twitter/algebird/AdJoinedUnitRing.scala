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

object AdjoinedRingSpecification extends Properties("AdjoinedRing") {
  import BaseProperties._

  implicit def adjoined[T:Arbitrary]: Arbitrary[AdjoinedUnit[T]] = Arbitrary {
    implicitly[Arbitrary[T]].arbitrary.map { t => AdjoinedUnit(t) }
  }

  property("AdjoinedUnit[Int] is a Ring") = ringLaws[AdjoinedUnit[Int]]
  property("AdjoinedUnit[Long] is a Ring") = ringLaws[AdjoinedUnit[Long]]
}
