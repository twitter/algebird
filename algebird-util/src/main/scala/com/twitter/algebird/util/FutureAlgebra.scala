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
import com.twitter.util.Future

/**
 * Monoid and Semigroup on util.Future.
 *
 * @author Sam Ritchie
 * @author Oscar Boykin
 */

class FutureSemigroup[T: Semigroup] extends Semigroup[Future[T]] {
  override def plus(l: Future[T], r: Future[T]): Future[T] =
    for(lv <- l; rv <- r) yield Semigroup.plus(lv, rv)
}

class FutureMonoid[T: Monoid] extends FutureSemigroup[T] with Monoid[Future[T]] {
  override def zero = Future.value(Monoid.zero)
}

class FutureGroup[T: Group] extends FutureMonoid[T] with Group[Future[T]] {
  override def negate(v: Future[T]): Future[T] = v.map { implicitly[Group[T]].negate(_) }
  override def minus(l: Future[T], r: Future[T]): Future[T] =
    for(lv <- l; rv <- r) yield Group.minus(lv, rv)
}

class FutureRing[T: Ring] extends FutureGroup[T] with Ring[Future[T]] {
  override def one = Future.value(Ring.one)
  override def times(l : Future[T], r: Future[T]) =
    for(lv <- l; rv <- r) yield Ring.times(lv, rv)
}

class FutureField[T: Field] extends FutureRing[T] with Field[Future[T]] {
  override def inverse(v: Future[T]) = v.map { implicitly[Field[T]].inverse(_) }
  override def div(l : Future[T], r: Future[T]) =
    for(lv <- l; rv <- r) yield Field.div(lv, rv)
}
