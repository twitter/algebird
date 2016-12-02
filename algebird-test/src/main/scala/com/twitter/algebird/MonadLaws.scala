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
import org.scalacheck.Prop.forAll
import scala.math.Equiv
import Monad.{ pureOp, operators }

/**
 * Basic Monad laws, useful for testing any monad.
 */

object MonadLaws {
  import BaseProperties._

  def defaultEq[T] = { (t0: T, t1: T) => (t0 == t1) }

  def leftIdentity[M[_], T, U](eq: (M[U], M[U]) => Boolean = defaultEq[M[U]])(implicit monad: Monad[M], arb: Arbitrary[T], arbfn: Arbitrary[(T) => M[U]]) = {
    implicit val equiv: Equiv[M[U]] = Equiv.fromFunction(eq)
    leftIdentityEquiv[M, T, U]
  }

  def rightIdentity[M[_], T](eq: (M[T], M[T]) => Boolean = defaultEq[M[T]])(implicit monad: Monad[M], arb: Arbitrary[M[T]]) = {
    implicit val equiv: Equiv[M[T]] = Equiv.fromFunction(eq)
    rightIdentityEquiv[M, T]
  }

  def associative[M[_], T, U, V](eq: (M[V], M[V]) => Boolean)(implicit monad: Monad[M], arb: Arbitrary[M[T]], fn1: Arbitrary[(T) => M[U]],
    fn2: Arbitrary[U => M[V]]) = {
    implicit val equiv: Equiv[M[V]] = Equiv.fromFunction(eq)
    associativeEquiv[M, T, U, V]
  }

  // Equiv versions:
  def leftIdentityEquiv[M[_], T, U](
    implicit monad: Monad[M], arb: Arbitrary[T], arbfn: Arbitrary[(T) => M[U]], equiv: Equiv[M[U]]) =
    forAll { (t: T, fn: T => M[U]) => Equiv[M[U]].equiv(t.pure[M].flatMap(fn), fn(t)) }

  def rightIdentityEquiv[M[_], T](implicit monad: Monad[M], arb: Arbitrary[M[T]], equiv: Equiv[M[T]]) =
    forAll { (mt: M[T]) => Equiv[M[T]].equiv(mt.flatMap { _.pure[M] }, mt) }

  def associativeEquiv[M[_], T, U, V](implicit monad: Monad[M], arb: Arbitrary[M[T]], fn1: Arbitrary[(T) => M[U]],
    fn2: Arbitrary[U => M[V]], equiv: Equiv[M[V]]) = forAll { (mt: M[T], f1: T => M[U], f2: U => M[V]) =>
    Equiv[M[V]].equiv(mt.flatMap(f1).flatMap(f2), mt.flatMap { t => f1(t).flatMap(f2) })
  }

  // Just generate a map and use that as a function:
  implicit def fnArb[M[_], T, U](implicit map: Arbitrary[Map[T, M[U]]],
    arbu: Arbitrary[M[U]]): Arbitrary[T => M[U]] = Arbitrary {
    for (
      m <- implicitly[Arbitrary[Map[T, M[U]]]].arbitrary;
      defu <- implicitly[Arbitrary[M[U]]].arbitrary
    ) yield ({ (t: T) => m.getOrElse(t, defu) })
  }

  def monadLaws[M[_], T, U, R](eq: (M[R], M[R]) => Boolean = defaultEq[M[R]])(implicit monad: Monad[M], arb: Arbitrary[M[T]], fn1: Arbitrary[(T) => M[U]],
    arbr: Arbitrary[M[R]], fn2: Arbitrary[U => M[R]], arbu: Arbitrary[U]) = {
    implicit val eq: Equiv[T] = Equiv.universal
    monadLawsEquiv[M, T, U, R]
  }

  def monadLawsEquiv[M[_], T, U, R](
    implicit monad: Monad[M], arb: Arbitrary[M[T]],
    equivT: Equiv[M[T]], equivU: Equiv[M[U]], equivR: Equiv[M[R]],
    fn1: Arbitrary[(T) => M[U]], arbr: Arbitrary[M[R]],
    fn2: Arbitrary[U => M[R]], arbu: Arbitrary[U]) =
    associativeEquiv[M, T, U, R] && rightIdentityEquiv[M, R] && leftIdentityEquiv[M, U, R]

  implicit def indexedSeqA[T](implicit arbl: Arbitrary[List[T]]): Arbitrary[IndexedSeq[T]] =
    Arbitrary { arbl.arbitrary.map { _.toIndexedSeq } }

  implicit def vectorA[T](implicit arbl: Arbitrary[List[T]]): Arbitrary[Vector[T]] =
    Arbitrary { arbl.arbitrary.map { l => Vector(l: _*) } }

  implicit def seqA[T](implicit arbl: Arbitrary[List[T]]): Arbitrary[Seq[T]] =
    Arbitrary { arbl.arbitrary.map { l => Seq(l: _*) } }

  implicit def someA[T](implicit arbl: Arbitrary[T]): Arbitrary[Some[T]] =
    Arbitrary { arbl.arbitrary.map { l => Some(l) } }

  implicit def identityA[T](implicit arbl: Arbitrary[T]): Arbitrary[Identity[T]] =
    Arbitrary { arbl.arbitrary.map { l => Identity(l) } }
}
