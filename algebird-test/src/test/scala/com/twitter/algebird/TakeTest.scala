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

  implicit def arbTake[T](implicit tarb: Arbitrary[T]): Arbitrary[TakeState[T]] = Arbitrary( for {
    s <- tarb.arbitrary
    c <- Gen.choose(0L, 100000L)
    b <- Gen.oneOf(true, false)
  } yield TakeState(s, c, b))

  implicit val takeSemi = new TakeSemigroup[Int](100)

  property("Take is a semigroup") = semigroupLaws[TakeState[Int]]

  def take[T](t: List[T], cnt: Long): List[T] = {
    implicit val takes = new TakeSemigroup[T](cnt)
    t.map(item => Option(TakeState(item)))
      .scanLeft(None: Option[TakeState[T]])(Monoid.plus[Option[TakeState[T]]](_, _))
      .collect { case Some(TakeState(item, _, false)) => item }
  }

  property("Take works as expected") = forAll { (t: List[Int]) =>
    val posCnt = Gen.choose(1, 2 * t.size + 2).sample.get
    t.take(posCnt) == take(t, posCnt)
  }
}
