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
 * Simple implementation of a Functor type-class.
 *
 * Laws Functors must follow: map(m)(id) == m map(m)(f andThen g) == map(map(m)(f))(g)
 */
@implicitNotFound(msg = "Cannot find Functor type class for ${M}")
trait Functor[M[_]] {
  def map[T, U](m: M[T])(fn: (T) => U): M[U]
}

/**
 * For use from Java/minimizing code bloat in scala
 */
abstract class AbstractFunctor[M[_]] extends Functor[M]

/**
 * Follows the type-class pattern for the Functor trait
 */
object Functor {

  /** Get the Functor for a type, e.g: Functor[List] */
  def apply[M[_]](implicit functor: Functor[M]): Functor[M] = functor
  def map[M[_], T, U](m: M[T])(fn: (T) => U)(implicit functor: Functor[M]): M[U] =
    functor.map(m)(fn)

  implicit def operators[A, M[_]](m: M[A])(implicit functor: Functor[M]): FunctorOperators[A, M] =
    new FunctorOperators(m)(functor)
}

/**
 * This enrichment allows us to use our Functor instances in for expressions: if (import Functor._) has been
 * done
 */
class FunctorOperators[A, M[_]](m: M[A])(implicit functor: Functor[M]) {
  // This is called fmap in haskell
  def map[U](fn: (A) => U): M[U] = functor.map(m)(fn)
}
