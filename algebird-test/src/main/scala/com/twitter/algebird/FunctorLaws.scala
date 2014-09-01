/*
Copyright 2014 Twitter, Inc.

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

import org.scalacheck.{ Arbitrary, Prop }
import org.scalacheck.Prop.forAll

/**
 * Basic Functor laws, useful for testing any functor.
 */
object FunctorLaws {
  import BaseProperties.{ HigherEq, DefaultHigherEq }

  def identityLaw[M[_], V](
    eq: HigherEq[M] = new DefaultHigherEq[M])(
      implicit functor: Functor[M],
      arb: Arbitrary[M[V]]): Prop =
    forAll { (mv: M[V]) => eq(functor.map(mv) { x => x }, mv) }

  def composeLaw[M[_], T, U, V](
    eq: HigherEq[M] = new DefaultHigherEq[M])(
      implicit functor: Functor[M],
      arb: Arbitrary[M[T]],
      arbFn1: Arbitrary[T => U],
      arbFn2: Arbitrary[U => V]): Prop =
    forAll { (mt: M[T], fn1: T => U, fn2: U => V) =>
      eq(
        functor.map(mt)(fn1 andThen fn2),
        functor.map(functor.map(mt)(fn1))(fn2))
    }

  def functorLaws[M[_], T, U, V](
    eq: HigherEq[M] = new DefaultHigherEq[M])(
      implicit functor: Functor[M],
      arbMt: Arbitrary[M[T]],
      arbMv: Arbitrary[M[V]],
      arbFn1: Arbitrary[T => U],
      arbFn2: Arbitrary[U => V]): Prop =
    identityLaw[M, V](eq) && composeLaw[M, T, U, V](eq)
}

