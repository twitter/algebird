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

import scala.annotation.implicitNotFound

/**
 * Simple implementation of an Applicative type-class.
 * There are many choices for the canonical second operation (join, sequence, joinWith, ap),
 * all equivalent. For a Functor modeling concurrent computations with failure, like Future,
 * combining results with join can save a lot of time over combining with flatMap. (Given two
 * operations, if the second fails before the first completes, one can fail the entire computation
 * right then. With flatMap, one would have to wait for the first operation to complete before
 * failing it.)
 *
 * Laws Applicatives must follow:
 *  map(apply(x))(f) == apply(f(x))
 *  join(apply(x), apply(y)) == apply((x, y))
 *  (sequence and joinWith specialize join - they should behave appropriately)
 */
@implicitNotFound(msg = "Cannot find Applicative type class for ${M}")
trait Applicative[M[_]] extends Functor[M] {
  // in haskell, called return, but that's a reserved word
  // constructs an Applicative instance from the given value, e.g. List(1)
  def apply[T](v: T): M[T]
  def join[T, U](mt: M[T], mu: M[U]): M[(T, U)]
  def sequence[T](ms: Seq[M[T]]): M[Seq[T]] =
    ms match {
      case Seq() => apply(Seq.empty)
      case Seq(m) => map(m) { Seq(_) }
      case Seq(m, n) => joinWith(m, n) { Seq(_, _) }
      case _ =>
        val mb =
          ms.foldLeft(apply(Seq.newBuilder[T])) { (mb, mt) =>
            joinWith(mb, mt) { (b, t) => b += t }
          }
        map(mb) { _.result }
    }
  def joinWith[T, U, V](mt: M[T], mu: M[U])(fn: (T, U) => V): M[V] =
    map(join(mt, mu)) { case (t, u) => fn(t, u) }

  def join[T1, T2, T3](m1: M[T1], m2: M[T2], m3: M[T3]): M[(T1, T2, T3)] =
    joinWith(join(m1, m2), m3) { case ((t1, t2), t3) => (t1, t2, t3) }

  def join[T1, T2, T3, T4](m1: M[T1], m2: M[T2], m3: M[T3], m4: M[T4]): M[(T1, T2, T3, T4)] =
    joinWith(join(join(m1, m2), m3), m4) { case (((t1, t2), t3), t4) => (t1, t2, t3, t4) }

  def join[T1, T2, T3, T4, T5](m1: M[T1], m2: M[T2], m3: M[T3], m4: M[T4], m5: M[T5]): M[(T1, T2, T3, T4, T5)] =
    joinWith(join(join(join(m1, m2), m3), m4), m5) { case ((((t1, t2), t3), t4), t5) => (t1, t2, t3, t4, t5) }
}

/**
 * For use from Java/minimizing code bloat in scala
 */
abstract class AbstractApplicative[M[_]] extends Applicative[M]

/**
 * Follows the type-class pattern for the Applicative trait
 */
object Applicative {
  /** Get the Applicative for a type, e.g: Applicative[List] */
  def apply[M[_]](implicit app: Applicative[M]): Applicative[M] = app
  def join[M[_], T, U](mt: M[T], mu: M[U])(implicit app: Applicative[M]): M[(T, U)] =
    app.join(mt, mu)
  def join[M[_], T1, T2, T3](m1: M[T1], m2: M[T2], m3: M[T3])(implicit app: Applicative[M]): M[(T1, T2, T3)] =
    app.join(m1, m2, m3)
  def join[M[_], T1, T2, T3, T4](m1: M[T1], m2: M[T2], m3: M[T3], m4: M[T4])(implicit app: Applicative[M]): M[(T1, T2, T3, T4)] =
    app.join(m1, m2, m3, m4)
  def join[M[_], T1, T2, T3, T4, T5](m1: M[T1], m2: M[T2], m3: M[T3], m4: M[T4], m5: M[T5])(implicit app: Applicative[M]): M[(T1, T2, T3, T4, T5)] =
    app.join(m1, m2, m3, m4, m5)
  def sequence[M[_], T](ms: Seq[M[T]])(implicit app: Applicative[M]): M[Seq[T]] =
    app.sequence(ms)
  def joinWith[M[_], T, U, V](mt: M[T], mu: M[U])(fn: (T, U) => V)(implicit app: Applicative[M]): M[V] =
    app.joinWith(mt, mu)(fn)

  // Set up the syntax magic (allow .pure[Int] syntax and flatMap in for):
  // import Applicative.{pureOp, operators} to get
  implicit def pureOp[A](a: A) = new PureOp(a)
  implicit def operators[A, M[_]](m: M[A])(implicit app: Applicative[M]) =
    new ApplicativeOperators(m)(app)
}

// This is the enrichment pattern to allow syntax like: 1.pure[List] == List(1)
// if we put a pure method in Applicative, it would take two type parameters, only one
// of which could be inferred, and it's annoying to write Applicative.pure[Int,List](1)
class PureOp[A](a: A) {
  def pure[M[_]](implicit app: Applicative[M]) = app(a)
}

/**
 * This enrichment allows us to use our Applicative instances in for expressions:
 * if (import Applicative._) has been done
 */
class ApplicativeOperators[A, M[_]](m: M[A])(implicit app: Applicative[M]) extends FunctorOperators[A, M](m) {
  def join[B](mb: M[B]): M[(A, B)] = app.join(m, mb)
  def joinWith[B, C](mb: M[B])(fn: (A, B) => C): M[C] = app.joinWith(m, mb)(fn)
}
