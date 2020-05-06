/*
Copyright 2015 Twitter, Inc.

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

class PreparerLaws extends CheckProperties {

  implicit def aggregator[A, B, C](implicit
      prepare: Arbitrary[A => B],
      sg: Semigroup[B],
      present: Arbitrary[B => C]
  ): Arbitrary[Aggregator[A, B, C]] = Arbitrary {
    for {
      pp <- prepare.arbitrary
      ps <- present.arbitrary
    } yield new Aggregator[A, B, C] {
      def prepare(a: A) = pp(a)
      def semigroup = sg
      def present(b: B) = ps(b)
    }
  }

  property("mapping before aggregate is correct") {
    forAll { (in: List[Int], compose: (Int => Int), ag: Aggregator[Int, Int, Int]) =>
      val composed = Preparer[Int].map(compose).aggregate(ag)
      in.isEmpty || composed(in) == ag(in.map(compose))
    }
  }

  property("split with two aggregators is correct") {
    forAll { (in: List[Int], ag1: Aggregator[Int, Set[Int], Int], ag2: Aggregator[Int, Unit, String]) =>
      val c = Preparer[Int].split(p => (p.aggregate(ag1), p.aggregate(ag2)))
      in.isEmpty || c(in) == ((ag1(in), ag2(in)))
    }
  }

  implicit def monoidAggregator[A, B, C](implicit
      prepare: Arbitrary[A => B],
      m: Monoid[B],
      present: Arbitrary[B => C]
  ): Arbitrary[MonoidAggregator[A, B, C]] =
    Arbitrary {
      for {
        pp <- prepare.arbitrary
        ps <- present.arbitrary
      } yield new MonoidAggregator[A, B, C] {
        def prepare(a: A) = pp(a)
        def monoid = m
        def present(b: B) = ps(b)
      }
    }

  property("flatten before aggregate is correct") {
    forAll { (in: List[List[Int]], ag: MonoidAggregator[Int, Int, Int]) =>
      val flattenedAg = Preparer[List[Int]].flatten.aggregate(ag)
      flattenedAg(in) == ag(in.flatten)
    }
  }

  property("map, flatMap, and split all together are correct") {
    forAll {
      (
          in: List[Int],
          mapFn: (Int => Int),
          flatMapFn: (Int => List[Int]),
          ag1: MonoidAggregator[Int, Int, Int],
          ag2: MonoidAggregator[Int, Int, Int]
      ) =>
        val ag =
          Preparer[Int]
            .map(mapFn)
            .flatMap(flatMapFn)
            .split(a => (a.aggregate(ag1), a.aggregate(ag2)))

        val preSplit = in.map(mapFn).flatMap(flatMapFn)
        in.isEmpty || ag(in) == ((ag1(preSplit), ag2(preSplit)))
    }
  }
}
