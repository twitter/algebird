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

package com.twitter.algebird
package util

import com.twitter.algebird.BaseProperties._
import com.twitter.algebird.MonadLaws.{ monadLawsEquiv, monadLaws }
import com.twitter.util.{ Await, Future, Return, Throw, Try }
import org.scalacheck.{ Arbitrary, Gen }
import scala.util.control.NonFatal

class UtilAlgebraProperties extends CheckProperties with UtilGenerators {
  def toOption[T](f: Future[T]): Option[T] =
    try {
      Some(Await.result(f))
    } catch {
      case NonFatal(_) => None
    }

  implicit def futureEquiv[T: Equiv]: Equiv[Future[T]] = Equiv.by(toOption)

  property("Future is a monad") {
    import UtilAlgebras.futureMonad
    monadLaws[Future, Int, String, Long]
  }

  property("Future[Long] is a commutative semigroup") {
    import UtilAlgebras.futureSemigroup
    commutativeSemigroupLaws[Future[Long]]
  }

  property("Future[Int] is a commutative monoid") {
    import UtilAlgebras.futureMonoid
    commutativeMonoidLaws[Future[Int]]
  }

  property("Try is a monad") {
    import UtilAlgebras.tryMonad
    monadLaws[Try, Int, String, Long]()
  }

  property("Try[Long] is a commutative semigroup") {
    import UtilAlgebras.trySemigroup
    commutativeSemigroupLaws[Try[Long]]
  }

  property("Try[Int] is a commutative monoid") {
    import UtilAlgebras.tryMonoid
    commutativeMonoidLaws[Try[Int]]
  }
}

trait UtilGenerators {
  def returnGen[T](g: Gen[T]): Gen[Return[T]] = g.map(Return(_))
  def throwGen[T](g: Gen[Exception]): Gen[Throw[T]] = g.map(Throw[T](_))
  def tryGen[T](s: Gen[T], f: Gen[Exception]): Gen[Try[T]] = Gen.oneOf(returnGen(s), throwGen(f))

  def futureGen[T](s: Gen[T], f: Gen[Exception]): Gen[Future[T]] = tryGen(s, f).map(Future.const[T](_))

  implicit def tryArb[T](implicit arb: Arbitrary[T], err: Arbitrary[Exception]): Arbitrary[Try[T]] =
    Arbitrary(tryGen(arb.arbitrary, err.arbitrary))

  implicit def futureArb[T](implicit arb: Arbitrary[T], err: Arbitrary[Exception]): Arbitrary[Future[T]] =
    Arbitrary(futureGen(arb.arbitrary, err.arbitrary))
}
