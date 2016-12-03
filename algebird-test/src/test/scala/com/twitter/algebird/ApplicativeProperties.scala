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

package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

class ApplicativeProperties extends CheckProperties {
  import ApplicativeLaws._
  import Monad._ // for Monad instances
  import MonadLaws._ // for Arbitrary instances

  property("list") {
    applicativeLaws[List, Int, String, Long]()
  }

  property("option") {
    applicativeLaws[Option, Int, String, Long]()
  }

  property("indexedseq") {
    applicativeLaws[IndexedSeq, Int, String, Long]()
  }

  property("vector") {
    applicativeLaws[Vector, Int, String, Long]()
  }

  property("set") {
    applicativeLaws[Set, Int, String, Long]()
  }

  property("seq") {
    applicativeLaws[Seq, Int, String, Long]()
  }

  property("identity") {
    applicativeLaws[Identity, Int, String, Long]()
  }

  property("sequenceGen") {
    // This follows from the laws, so we are just testing
    // the implementation of sequenceGen against sequence here
    forAll { ls: List[Option[Int]] =>
      val res: Option[Vector[Int]] = Applicative.sequenceGen(ls)
      Applicative.sequence(ls).map(_.toVector) == res
    }
  }
  // Applicative algebras:
  import BaseProperties._
  property("Applicative Semigroup") {
    implicit val optSg = new ApplicativeSemigroup[Int, Option]
    implicit val listSg = new ApplicativeSemigroup[String, List]
    // the + here is actually a cross-product, and testing sumOption blows up
    semigroupLaws[Option[Int]] && isAssociative[List[String]]
  }

  property("Applicative Monoid") {
    implicit val optSg = new ApplicativeMonoid[Int, Option]
    implicit val listSg = new ApplicativeMonoid[String, List]
    // the + here is actually a cross-product, and testing sumOption blows up
    monoidLaws[Option[Int]] && validZero[List[String]]
  }

  // These laws work for only "non-empty" monads
  property("Applicative Group") {
    implicit val optSg = new ApplicativeGroup[Int, Some]
    groupLaws[Some[Int]]
  }

  property("Applicative Ring") {
    implicit val optSg = new ApplicativeRing[Int, Some]
    ringLaws[Some[Int]]
  }
}
