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

import org.scalacheck.{Arbitrary, Prop}
import org.scalacheck.Prop.forAll

/**
 * Basic Applicative laws, useful for testing any app.
 */
object ApplicativeLaws {
  import BaseProperties.{DefaultHigherEq, HigherEq}

  def applyLaw[M[_], T, U](eq: HigherEq[M] = new DefaultHigherEq[M])(implicit
                                                                     app: Applicative[M],
                                                                     arb: Arbitrary[T],
                                                                     arbFn: Arbitrary[T => U]): Prop =
    forAll { (t: T, fn: T => U) =>
      eq(app.map(app.apply(t))(fn), app.apply(fn(t)))
    }

  def joinLaw[M[_], T, U](eq: HigherEq[M] = new DefaultHigherEq[M])(implicit
                                                                    app: Applicative[M],
                                                                    arb1: Arbitrary[T],
                                                                    arb2: Arbitrary[U]): Prop =
    forAll { (t: T, u: U) =>
      eq(app.join(app.apply(t), app.apply(u)), app.apply((t, u)))
    }

  // These follow from apply and join:

  def sequenceLaw[M[_], T](eq: HigherEq[M] = new DefaultHigherEq[M])(implicit
                                                                     app: Applicative[M],
                                                                     arb: Arbitrary[Seq[T]]): Prop =
    forAll { (ts: Seq[T]) =>
      eq(app.sequence(ts.map { app.apply(_) }), app.apply(ts))
    }

  def joinWithLaw[M[_], T, U, V](eq: HigherEq[M] = new DefaultHigherEq[M])(
      implicit
      app: Applicative[M],
      arbT: Arbitrary[T],
      arbU: Arbitrary[U],
      arbJoinFn: Arbitrary[(T, U) => V]): Prop =
    forAll { (t: T, u: U, fn: (T, U) => V) =>
      eq(app.joinWith(app.apply(t), app.apply(u))(fn), app.apply(fn(t, u)))
    }

  def applicativeLaws[M[_], T, U, V](eq: HigherEq[M] = new DefaultHigherEq[M])(
      implicit
      app: Applicative[M],
      arbMt: Arbitrary[T],
      arbMts: Arbitrary[Seq[T]],
      arbMu: Arbitrary[U],
      arbFn: Arbitrary[T => U],
      arbJoinFn: Arbitrary[(T, U) => V]): Prop =
    applyLaw[M, T, U](eq) &&
      joinLaw[M, T, U](eq) &&
      sequenceLaw[M, T](eq) &&
      joinWithLaw[M, T, U, V](eq)
}
