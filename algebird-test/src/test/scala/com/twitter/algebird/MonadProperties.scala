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

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

import Monad.{ pureOp, operators }

class MonadProperties extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._
  import MonadLaws._

  property("list") {
    monadLaws[List, Int, String, Long]()
  }

  property("option") {
    monadLaws[Option, Int, String, Long]()
  }

  property("indexedseq") {
    monadLaws[IndexedSeq, Int, String, Long]()
  }

  property("vector") {
    monadLaws[Vector, Int, String, Long]()
  }

  property("set") {
    monadLaws[Set, Int, String, Long]()
  }

  property("seq") {
    monadLaws[Seq, Int, String, Long]()
  }

  // Monad algebras:
  property("Monad Semigroup") {
    implicit val optSg = new MonadSemigroup[Int, Option]
    implicit val listSg = new MonadSemigroup[String, List]
    // the + here is actually a cross-product, and testing sumOption blows up
    semigroupLaws[Option[Int]]
    isAssociative[List[String]]
  }

  property("Monad Monoid") {
    implicit val optSg = new MonadMonoid[Int, Option]
    implicit val listSg = new MonadMonoid[String, List]
    // the + here is actually a cross-product, and testing sumOption blows up
    monoidLaws[Option[Int]]
    validZero[List[String]]
  }

  // These laws work for only "non-empty" monads
  property("Monad Group") {
    implicit val optSg = new MonadGroup[Int, Some]
    groupLaws[Some[Int]]
  }

  property("Monad Ring") {
    implicit val optSg = new MonadRing[Int, Some]
    ringLaws[Some[Int]]
  }

  property("Monad Field") {
    implicit val optSg = new MonadField[Boolean, Some]
    fieldLaws[Some[Boolean]]
  }
}
