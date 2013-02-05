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

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose
import org.scalacheck.Prop.forAll

import Monad.{pureOp, operators}

object MonadProperties extends Properties("Monad") {
  import BaseProperties._
  import MonadLaws._

  def defaultEq[T] = {(t0: T, t1: T) => (t0 == t1)}

  def leftIdentity[M[_],T,U](eq: (M[U],M[U]) => Boolean = defaultEq[M[U]])
    (implicit monad: Monad[M], arb: Arbitrary[T], arbfn: Arbitrary[(T) => M[U]]) =
      forAll { (t: T, fn: T => M[U]) => eq(t.pure[M].flatMap(fn), fn(t)) }

  def rightIdentity[M[_],T](eq: (M[T],M[T]) => Boolean = defaultEq[M[T]])
    (implicit monad: Monad[M], arb: Arbitrary[M[T]]) =
    forAll { (mt: M[T]) => eq(mt.flatMap { _.pure[M] }, mt) }

  def associative[M[_],T,U,V](eq: (M[V],M[V]) => Boolean)
    (implicit monad: Monad[M], arb: Arbitrary[M[T]], fn1: Arbitrary[(T) => M[U]],
      fn2: Arbitrary[U => M[V]]) = forAll { (mt: M[T], f1: T => M[U], f2: U => M[V]) =>
    eq(mt.flatMap(f1).flatMap(f2), mt.flatMap { t => f1(t).flatMap(f2) })
  }

  // Just generate a map and use that as a function:
  implicit def fnArb[M[_],T,U](implicit map: Arbitrary[Map[T,M[U]]],
    arbu: Arbitrary[M[U]]):
    Arbitrary[T => M[U]] = Arbitrary {
       for( m <- implicitly[Arbitrary[Map[T,M[U]]]].arbitrary;
         defu <- implicitly[Arbitrary[M[U]]].arbitrary)
         yield ({(t: T) => m.getOrElse(t, defu)})
    }

  def monadLaws[M[_],T,U,R](eq: (M[R],M[R]) => Boolean = defaultEq[M[R]])
    (implicit monad: Monad[M], arb: Arbitrary[M[T]], fn1: Arbitrary[(T) => M[U]],
      arbr: Arbitrary[M[R]], fn2: Arbitrary[U => M[R]], arbu: Arbitrary[U]) =
      associative[M,T,U,R](eq) && rightIdentity[M,R](eq) && leftIdentity[M,U,R](eq)

  implicit def indexedSeqA[T](implicit arbl: Arbitrary[List[T]]): Arbitrary[IndexedSeq[T]] =
    Arbitrary { arbl.arbitrary.map { _.toIndexedSeq } }

  implicit def vectorA[T](implicit arbl: Arbitrary[List[T]]): Arbitrary[Vector[T]] =
    Arbitrary { arbl.arbitrary.map { l => Vector(l: _*) } }

  implicit def seqA[T](implicit arbl: Arbitrary[List[T]]): Arbitrary[Seq[T]] =
    Arbitrary { arbl.arbitrary.map { l => Seq(l: _*) } }

  implicit def someA[T](implicit arbl: Arbitrary[T]): Arbitrary[Some[T]] =
    Arbitrary { arbl.arbitrary.map { l => Some(l) } }

  property("list") = monadLaws[List, Int, String, Long]()
  property("option") = monadLaws[Option, Int, String, Long]()
  property("indexedseq") = monadLaws[IndexedSeq, Int, String, Long]()
  property("vector") = monadLaws[Vector, Int, String, Long]()
  property("set") = monadLaws[Set, Int, String, Long]()
  property("seq") = monadLaws[Seq, Int, String, Long]()

  // Monad algebras:
  property("Monad Semigroup") = {
    implicit val optSg = new MonadSemigroup[Int, Option]
    implicit val listSg = new MonadSemigroup[String, List]
    semigroupLaws[Option[Int]] && semigroupLaws[List[String]]
  }
  property("Monad Monoid") = {
    implicit val optSg = new MonadMonoid[Int, Option]
    implicit val listSg = new MonadMonoid[String, List]
    monoidLaws[Option[Int]] && monoidLaws[List[String]]
  }
  // These laws work for only "non-empty" monads
  property("Monad Group") = {
    implicit val optSg = new MonadGroup[Int, Some]
    groupLaws[Some[Int]]
  }
  property("Monad Ring") = {
    implicit val optSg = new MonadRing[Int, Some]
    ringLaws[Some[Int]]
  }
  property("Monad Field") = {
    implicit val optSg = new MonadField[Boolean, Some]
    fieldLaws[Some[Boolean]]
  }
}
