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

import com.twitter.algebird.{ Semigroup, Monoid, Group, Ring, Field }
import com.twitter.util.{Future, Try}

object Algebras {
  implicit def futureSemigroup[T:Semigroup]: Semigroup[Future[T]] = new FutureSemigroup[T]
  implicit def futureMonoid[T:Monoid]: Monoid[Future[T]] = new FutureMonoid[T]
  implicit def futureGroup[T:Group]: Group[Future[T]] = new FutureGroup[T]
  implicit def futureRing[T:Ring]: Ring[Future[T]] = new FutureRing[T]
  implicit def futureField[T:Field]: Field[Future[T]] = new FutureField[T]

  implicit def trySemigroup[T:Semigroup]: Semigroup[Try[T]] = new TrySemigroup[T]
  implicit def tryMonoid[T:Monoid]: Monoid[Try[T]] = new TryMonoid[T]
  implicit def tryGroup[T:Group]: Group[Try[T]] = new TryGroup[T]
  implicit def tryRing[T:Ring]: Ring[Try[T]] = new TryRing[T]
  implicit def tryField[T:Field]: Field[Try[T]] = new TryField[T]
}
