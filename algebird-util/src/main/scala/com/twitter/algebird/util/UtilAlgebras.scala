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
    def apply[T](v: T) = Future.value(v)
    def flatMap[T, U](m: Future[T])(fn: T => Future[U]) = m.flatMap(fn)
    /*
     * We override join with more efficient implementations than
     * just using flatMap
     */
    override def join[T, U](a: Future[T], b: Future[U]): Future[(T, U)] =
      a.join(b)
    override def join[T, U, V](t: Future[T], u: Future[U], v: Future[V]) =
      Future.join(t, u, v)
    override def join[T, U, V, W](t: Future[T], u: Future[U], v: Future[V], w: Future[W]) =
      Future.join(t, u, v, w)
    override def join[T, U, V, W, X](t: Future[T], u: Future[U], v: Future[V], w: Future[W], x: Future[X]) =
      Future.join(t, u, v, w, x)
    override def map[T, U](m: Future[T])(fn: T => U) = m.map(fn)
    /*
     * We override sequence with more efficient implementations than
     * just using flatMap
     */
    override def sequence[T](fs: Seq[Future[T]]): Future[Seq[T]] = Future.collect(fs)
  }
  implicit val tryMonad: Monad[Try] = new Monad[Try] {
    def apply[T](v: T) = Return(v)
    override def map[T, U](m: Try[T])(fn: T => U) = m.map(fn)
    def flatMap[T, U](m: Try[T])(fn: T => Try[U]) = m.flatMap(fn)
  }

  implicit def futureSemigroup[T: Semigroup]: Semigroup[Future[T]] = new ApplicativeSemigroup[T, Future]
  implicit def futureMonoid[T: Monoid]: Monoid[Future[T]] = new ApplicativeMonoid[T, Future]

  implicit def trySemigroup[T: Semigroup]: Semigroup[Try[T]] = new ApplicativeSemigroup[T, Try]
  implicit def tryMonoid[T: Monoid]: Monoid[Try[T]] = new ApplicativeMonoid[T, Try]

  @deprecated("futureGroup is broken and will be removed at the next minor version bump", since = "0.12.3")
  implicit def futureGroup[T: Group]: Group[Future[T]] = new ApplicativeGroup[T, Future]

  @deprecated("futureRing is broken and will be removed at the next minor version bump", since = "0.12.3")
  implicit def futureRing[T: Ring]: Ring[Future[T]] = new ApplicativeRing[T, Future]

  @deprecated("futureField is broken and will be removed at the next minor version bump", since = "0.12.3")
  implicit def futureField[T: Field]: Field[Future[T]] = new ApplicativeField[T, Future]

  @deprecated("tryGroup is broken and will be removed at the next minor version bump", since = "0.12.3")
  implicit def tryGroup[T: Group]: Group[Try[T]] = new ApplicativeGroup[T, Try]

  @deprecated("tryRing is broken and will be removed at the next minor version bump", since = "0.12.3")
  implicit def tryRing[T: Ring]: Ring[Try[T]] = new ApplicativeRing[T, Try]

  @deprecated("tryField is broken and will be removed at the next minor version bump", since = "0.12.3")
  implicit def tryField[T: Field]: Field[Try[T]] = new ApplicativeField[T, Try]
}
