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
import org.scalacheck.Gen
import org.scalacheck.Properties
import org.scalacheck.Prop._

object AggregatorLaws extends Properties("Aggregator") {
  import BaseProperties._

  implicit def aggregator[A,B,C](implicit prepare: Arbitrary[A => B], sg: Semigroup[B], present: Arbitrary[B => C]):
    Arbitrary[Aggregator[A,B,C]] = Arbitrary { for {
      pp <- prepare.arbitrary
      ps <- present.arbitrary
    } yield new Aggregator[A,B,C] {
      def prepare(a:A)= pp(a)
      def reduce(l:B, r:B) = sg.plus(l,r)
      def present(b:B) = ps(b)
    }
  }

  property("composing before Aggregator is correct") = forAll { (in: List[Int], compose: (Int => Int), ag: Aggregator[Int,Int,Int]) =>
    val composed = ag.composePrepare(compose)
    in.isEmpty || composed(in) == ag(in.map(compose))
  }
  property("andThen after Aggregator is correct") = forAll { (in: List[Int], andt: (Int => Int), ag: Aggregator[Int,Int,Int]) =>
    val ag1 = ag.andThenPresent(andt)
    in.isEmpty || ag1(in) == andt(ag(in))
  }
  property("composing two Aggregators is correct") = forAll { (in: List[Int], ag1: Aggregator[Int,Int,Int], ag2:
    Aggregator[Int,Double,String]) =>
    val c = GeneratedTupleAggregator.from2(ag1, ag2)
    in.isEmpty || c(in) == (ag1(in), ag2(in))
  }
}
