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

import org.specs._

class DecayedMapTest extends Specification {
  "A DecayedMap monoid" should {
    implicit val dmMonoid = DecayedMap.monoidWithEpsilon[Int](0.01)

    "decay two maps properly" in {
      val dm1 = DecayedMap.build(Map(1 -> 1.0), 0.0, 1.0)
      val dm2 = DecayedMap.build(Map(1 -> 1.0), 1.0, 1.0)
      val dmOut = Monoid.plus(dm1, dm2)
      dmOut.innerMap must_== Map(1 -> 1.5)
    }

    "remove elements that are less than epsilon" in {
      val dm1 = DecayedMap.build(Map(2 -> 0.01), 0.0, 1.0)
      val dm2 = DecayedMap.build(Map(1 -> 1.0), 1.0, 1.0)
      val dmOut = Monoid.plus(dm1, dm2)
      dmOut.innerMap must_== Map(1 -> 1.0)
    }
  }
}
