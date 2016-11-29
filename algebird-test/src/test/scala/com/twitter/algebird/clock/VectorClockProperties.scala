/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.algebird.clock

import com.twitter.algebird._

import com.twitter.algebird.clock.VectorClock._

import org.scalacheck.{ Arbitrary, Gen, Properties }
import org.scalacheck.Prop._

object VectorClockProperties extends Properties("VectorClock") {

  import BaseProperties._

  def genStamp: Gen[Stamp] = for {
    size <- Gen.choose(0, 1000)
    id <- Gen.choose(0, size)
  } yield Stamp(Set(id), Vector.fill(size + 1)(0L))

  implicit def arbStamp: Arbitrary[Stamp] = Arbitrary(genStamp)

  property("Stamp is successible") = SuccessibleLaws.successibleLaws[Stamp]
  property("Stamp has a commutative idempotent semigroup") = commutativeBandLaws[Stamp]
  property("Stamp fork + join is identity") = forAll { s: Stamp =>
    Equiv[Stamp].equiv(s.forkAll.reduce(_.join(_)), s)
  }

  property("Stamp is a Clock") = ClockLaws[Stamp]

  property("Clocks.notLTEQSemigroup[Stamp, Long] is a PartialSemigroup") =
    ClockLaws.notLTEQIsPartialSemigroup[Stamp, Long]

  property("Clocks.isGTSemigroup[Stamp, Long] is a PartialSemigroup") =
    ClockLaws.isGTIsPartialSemigroup[Stamp, Long]

  property("Clocks.notLTEQSemigroup[Stamp, Long] respects time order") =
    ClockLaws.partialSemiIsOrdered[Stamp, Long]
}
