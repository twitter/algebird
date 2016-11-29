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

import com.twitter.algebird.clock.IntervalTree._

import org.scalacheck.{ Arbitrary, Gen, Properties }
import org.scalacheck.Prop._

object IntervalTreeProperties extends Properties("IntervalTree") {

  import BaseProperties._

  def genId: Gen[Id] = Gen.oneOf(ZeroId, OneId, Gen.lzy(genTreeId))

  def genTreeId: Gen[Id] = for {
    left <- genId
    right <- genId
  } yield (TreeId(left, right).normalize)

  implicit def arbId: Arbitrary[Id] = Arbitrary(genId)

  // Bias to constants to avoid the tree getting too deep
  def genEvent: Gen[Event] = Gen.frequency((3, genConst), (1, Gen.lzy(genTreeEvent)))

  def genConst: Gen[ConstEvent] = Gen.choose(0L, 1000L).map(ConstEvent(_))

  def genTreeEvent: Gen[Event] = for {
    height <- Gen.choose(0L, Int.MaxValue.toLong)
    left <- genEvent
    right <- genEvent
  } yield (TreeEvent(height, left, right).normalize)

  implicit def arbEvent: Arbitrary[Event] = Arbitrary(genEvent)

  def genStamp: Gen[Stamp] = for {
    id <- genId
    ev <- genEvent
  } yield Stamp(id, ev)

  implicit def arbStamp: Arbitrary[Stamp] = Arbitrary(genStamp)

  property("Ids have a semiring") = semiringLaws[Id]
  property("Ids have a commutative band") = commutativeBandLaws[Id]

  case class ZeroOne(get: Double)
  implicit def zeroOne: Arbitrary[ZeroOne] = Arbitrary(Gen.choose(0.0, 1.0).map(ZeroOne(_)))
  property("Id plus is pointwise ||") = forAll { (e1: Id, e2: Id, zo: ZeroOne) =>
    val inner = zo.get
    val e3 = Monoid.plus(e1, e2)
    e3(inner) == (e1(inner) || e2(inner))
  }
  property("Id times is pointwise &&") = forAll { (e1: Id, e2: Id, zo: ZeroOne) =>
    val inner = zo.get
    val e3 = Ring.times(e1, e2)
    e3(inner) == (e1(inner) && e2(inner))
  }
  property("Event plus is pointwise max") = forAll { (e1: Event, e2: Event, zo: ZeroOne) =>
    val inner = zo.get
    val e3 = Monoid.plus(e1, e2)
    e3(inner) == math.max(e1(inner), e2(inner))
  }
  property("Events have a commutative idempotent monoid") = commutativeMonoidLaws[Event] &&
    commutativeBandLaws[Event]

  property("Events.normalize does not increase depth") = forAll { (e: Event) =>
    e.depth <= (e.normalize.depth)
  }
  property("Id.normalize does not increase depth") = forAll { (id: Id) =>
    id.depth <= (id.normalize.depth)
  }
  property("Stamp is successible") = SuccessibleLaws.successibleLaws[Stamp]
  property("Stamp has a commutative idempotent semigroup") = commutativeBandLaws[Stamp]
  property("Stamp fork + join is identity") = forAll { s: Stamp =>
    val (l, r) = s.fork
    Equiv[Stamp].equiv(l.join(r), s)
  }
  property("Stamp forkMany(n).size == n") = forAll(Gen.choose(1, 100), genStamp) { (n, s) =>
    s.forkMany(n).size == n
  }
  property("Stamp Semigroup.sumOption(s.forkMany(n)) == Some(s)") =
    forAll(Gen.choose(1, 100), genStamp) { (n, s) =>
      Semigroup.sumOption(s.forkMany(n)) == Some(s)
    }

  property("Stamp is a Clock") = ClockLaws[Stamp]

  property("Clocks.notLTEQSemigroup[Stamp, Long] is a PartialSemigroup") =
    ClockLaws.notLTEQIsPartialSemigroup[Stamp, Long]

  property("Clocks.isGTSemigroup[Stamp, Long] is a PartialSemigroup") =
    ClockLaws.isGTIsPartialSemigroup[Stamp, Long]

  property("Clocks.notLTEQSemigroup[Stamp, Long] respects time order") =
    ClockLaws.partialSemiIsOrdered[Stamp, Long]
}
