/*
Copyright 2013 Twitter, Inc.

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
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.oneOf
import org.scalacheck.Gen

import scala.annotation.tailrec

object TakeTest extends Properties("TakeSemigroup") {
  import BaseProperties._

  implicit def arbTake[T](implicit tarb: Arbitrary[T]): Arbitrary[TakeState[(T, Long)]] = Arbitrary( for {
    s <- tarb.arbitrary
    c <- Gen.choose(0L, 100000L)
    b <- Gen.oneOf(true, false)
  } yield TakeState((s, c), b))

  implicit def takeSemi[T:Semigroup] = new TakeSemigroup[T](100)

  implicit def arbLast[T](implicit tarb: Arbitrary[T]): Arbitrary[Last[T]] =
    Arbitrary(tarb.arbitrary.map(Last(_)))

  property("Take is a semigroup") = semigroupLaws[TakeState[(Last[Int], Long)]]
  property("Take while summing is a semigroup") = semigroupLaws[TakeState[(Int, Long)]]

  def take[T](t: List[T], cnt: Long): List[T] = {
    implicit val takes: Semigroup[TakeState[(Last[T], Long)]] = new TakeSemigroup[Last[T]](cnt)
    t.map(item => Option(TakeState((Last(item), 1L))))
      .scanLeft(None: Option[TakeState[(Last[T], Long)]])(Monoid.plus[Option[TakeState[(Last[T], Long)]]](_, _))
      .collect { case Some(TakeState((Last(item), _), false)) => item }
  }

  property("Take works as expected") = forAll { (t: List[Int]) =>
    val posCnt = Gen.choose(1, 2 * t.size + 2).sample.get
    t.take(posCnt) == take(t, posCnt)
  }

  implicit val arbTakeD: Arbitrary[TakeState[BigInt]] =
    Arbitrary(Arbitrary.arbitrary[BigInt].map(b => TakeState(b.abs)))

  implicit val threshold: Semigroup[TakeState[BigInt]] = TakeWhileSemigroup[BigInt](_ < 100L)
  property("Takewhile sum < 100 is a semigroup") = semigroupLaws[TakeState[BigInt]]
}
