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
import com.twitter.util.{Try, Return, Throw}

/**
 * Monoid and Semigroup on util.Try.
 *
 * @author Sam Ritchie
 * @author Oscar Boykin
 */

// Clearly all these are generalizations of a Monad pattern with a single value:

class TrySemigroup[T: Semigroup] extends Semigroup[Try[T]] {
  override def plus(l: Try[T], r: Try[T]): Try[T] =
    for(lv <- l; rv <- r) yield Semigroup.plus(lv, rv)
}

class TryMonoid[T: Monoid] extends TrySemigroup[T] with Monoid[Try[T]] {
  override def zero = Return(Monoid.zero)
}

class TryGroup[T: Group] extends TryMonoid[T] with Group[Try[T]] {
  override def negate(v: Try[T]): Try[T] = v.map { implicitly[Group[T]].negate(_) }
  override def minus(l: Try[T], r: Try[T]): Try[T] =
    for(lv <- l; rv <- r) yield Group.minus(lv, rv)
}

class TryRing[T: Ring] extends TryGroup[T] with Ring[Try[T]] {
  override def one = Return(Ring.one)
  override def times(l : Try[T], r: Try[T]) =
    for(lv <- l; rv <- r) yield Ring.times(lv, rv)
}

class TryField[T: Field] extends TryRing[T] with Field[Try[T]] {
  override def inverse(v: Try[T]) = v.map { implicitly[Field[T]].inverse(_) }
  override def div(l : Try[T], r: Try[T]) =
    for(lv <- l; rv <- r) yield Field.div(lv, rv)
}
