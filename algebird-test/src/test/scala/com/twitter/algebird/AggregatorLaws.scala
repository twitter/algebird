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
import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks

class AggregatorLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  implicit def aggregator[A, B, C](implicit prepare: Arbitrary[A => B], sg: Semigroup[B], present: Arbitrary[B => C]): Arbitrary[Aggregator[A, B, C]] = Arbitrary {
    for {
      pp <- prepare.arbitrary
      ps <- present.arbitrary
    } yield new Aggregator[A, B, C] {
      def prepare(a: A) = pp(a)
      def semigroup = sg
      def present(b: B) = ps(b)
    }
  }

  property("composing before Aggregator is correct") {
    forAll { (in: List[Int], compose: (Int => Int), ag: Aggregator[Int, Int, Int]) =>
      val composed = ag.composePrepare(compose)
      assert(in.isEmpty || composed(in) == ag(in.map(compose)))
    }
  }

  property("andThen after Aggregator is correct") {
    forAll { (in: List[Int], andt: (Int => Int), ag: Aggregator[Int, Int, Int]) =>
      val ag1 = ag.andThenPresent(andt)
      assert(in.isEmpty || ag1(in) == andt(ag(in)))
    }
  }

  property("composing two Aggregators is correct") {
    forAll { (in: List[Int], ag1: Aggregator[Int, String, Int], ag2: Aggregator[Int, Int, String]) =>
      val c = GeneratedTupleAggregator.from2(ag1, ag2)
      assert(in.isEmpty || c(in) == (ag1(in), ag2(in)))
    }
  }
  property("Applicative composing two Aggregators is correct") {
    forAll { (in: List[Int], ag1: Aggregator[Int, Set[Int], Int], ag2: Aggregator[Int, Unit, String]) =>
      type AggInt[T] = Aggregator[Int, _, T]
      val c = Applicative.join[AggInt, Int, String](ag1, ag2)
      assert(in.isEmpty || c(in) == (ag1(in), ag2(in)))
    }
  }

  property("Aggregator.lift works for empty sequences") {
    forAll { (in: List[Int], ag: Aggregator[Int, Int, Int]) =>
      val liftedAg = ag.lift
      assert(in.isEmpty && liftedAg(in) == None || liftedAg(in) == Some(ag(in)))
    }
  }

  implicit def monoidAggregator[A, B, C](implicit prepare: Arbitrary[A => B], m: Monoid[B], present: Arbitrary[B => C]): Arbitrary[MonoidAggregator[A, B, C]] = Arbitrary {
    for {
      pp <- prepare.arbitrary
      ps <- present.arbitrary
    } yield new MonoidAggregator[A, B, C] {
      def prepare(a: A) = pp(a)
      def monoid = m
      def present(b: B) = ps(b)
    }
  }

  property("MonoidAggregator.sumBefore is correct") {
    forAll{ (in: List[List[Int]], ag: MonoidAggregator[Int, Int, Int]) =>
      val liftedAg = ag.sumBefore
      assert(liftedAg(in) == ag(in.flatten))
    }
  }
}
