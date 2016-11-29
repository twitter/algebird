/*
 * Copyright 2016 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.algebird.util

import com.twitter.algebird.CheckProperties
import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.MonadLaws.{ monadLawsEquiv, monadLaws }
import com.twitter.util.{ Await, Future, Return, Throw, Try }
import org.scalacheck.{ Arbitrary, Gen }
import scala.util.control.NonFatal

import UtilAlgebras._

class UtilAlgebraProperties extends CheckProperties with UtilGenerators {
  def toOption[T](f: Future[T]): Option[T] =
    try {
      Some(Await.result(f))
    } catch {
      case NonFatal(_) => None
    }

  implicit def futureEquiv[T: Equiv]: Equiv[Future[T]] = Equiv.by(toOption)

  property("Future is a monad") { monadLawsEquiv[Future, Int, String, Long] }

  property("Future[Int] is a commutative monoid") {
    commutativeMonoidLawsEquiv[Future[Int]]
  }

  property("Try is a monad") { monadLaws[Try, Int, String, Long]() }

  property("Try[Int] is a monoid") { monoidLaws[Try[Int]] }
}

trait UtilGenerators {
  def returnGen[T](g: Gen[T]): Gen[Return[T]] = g.map(Return(_))
  def throwGen[T](g: Gen[Throwable]): Gen[Throw[T]] = g.map(Throw[T](_))
  def tryGen[T](s: Gen[T], f: Gen[Throwable]): Gen[Try[T]] = Gen.oneOf(returnGen(s), throwGen(f))

  def futureGen[T](s: Gen[T], f: Gen[Throwable]): Gen[Future[T]] = tryGen(s, f).map(Future.const[T](_))

  implicit def tryArb[T](implicit arb: Arbitrary[T], err: Arbitrary[Throwable]): Arbitrary[Try[T]] =
    Arbitrary(tryGen(arb.arbitrary, err.arbitrary))

  implicit def futureArb[T](implicit arb: Arbitrary[T], err: Arbitrary[Throwable]): Arbitrary[Future[T]] =
    Arbitrary(futureGen(arb.arbitrary, err.arbitrary))
}
