/*
 * Copyright 2013 Twitter Inc.
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

import com.twitter.algebird._
import com.twitter.util.{ Future, Return, Try }

object UtilAlgebras {
  implicit val futureMonad: Monad[Future] = new Monad[Future] {
    def apply[T](v: T) = Future.value(v);
    def flatMap[T, U](m: Future[T])(fn: T => Future[U]) = m.flatMap(fn)
  }
  implicit val tryMonad: Monad[Try] = new Monad[Try] {
    def apply[T](v: T) = Return(v);
    def flatMap[T,U](m: Try[T])(fn: T => Try[U]) = m.flatMap(fn)
  }

  implicit def futureSemigroup[T:Semigroup]: Semigroup[Future[T]] = new MonadSemigroup[T, Future]
  implicit def futureMonoid[T:Monoid]: Monoid[Future[T]] = new MonadMonoid[T, Future]
  implicit def futureGroup[T:Group]: Group[Future[T]] = new MonadGroup[T, Future]
  implicit def futureRing[T:Ring]: Ring[Future[T]] = new MonadRing[T, Future]
  implicit def futureField[T:Field]: Field[Future[T]] = new MonadField[T, Future]

  implicit def trySemigroup[T:Semigroup]: Semigroup[Try[T]] = new MonadSemigroup[T, Try]
  implicit def tryMonoid[T:Monoid]: Monoid[Try[T]] = new MonadMonoid[T, Try]
  implicit def tryGroup[T:Group]: Group[Try[T]] = new MonadGroup[T, Try]
  implicit def tryRing[T:Ring]: Ring[Try[T]] = new MonadRing[T, Try]
  implicit def tryField[T:Field]: Field[Try[T]] = new MonadField[T, Try]
}
